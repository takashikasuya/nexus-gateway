"""bacpypes3-backed BACnetClient implementation."""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable

from bacpypes3.app import Application
from bacpypes3.basetypes import ErrorType
from bacpypes3.constructeddata import AnyAtomic
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Null
from bacpypes3.apdu import (
    ReadAccessSpecification,
    PropertyReference,
    PropertyIdentifier,
)
from bacpypes3.primitivedata import Real
from bacpypes3.vendor import VendorInfo

from bacnet_connector.connector import BACnetClient

logger = logging.getLogger(__name__)


def _to_float(raw: object) -> float | None:
    """Coerce a bacpypes3 primitive value to float, or return None."""
    if raw is None or isinstance(raw, (Null, ErrorType)):
        return None
    try:
        return float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


class Bacpypes3Client(BACnetClient):
    """BACnetClient backed by a bacpypes3 Application."""

    def __init__(self, app: Application):
        self._app = app
        # Map of (address, obj_id) -> (asyncio.Task, cancel_event) for active COV subs.
        self._cov_tasks: dict[tuple[str, str], asyncio.Task] = {}

    @classmethod
    async def create(cls, local_address: str) -> "Bacpypes3Client":
        app = await Application.create(
            local_address=local_address,
            device_object=None,  # client-only, no device object needed
        )
        return cls(app)

    async def read_property_multiple(
        self,
        address: str,
        device_id: int,
        requests: list[tuple[str, str]],
    ) -> list[tuple[str, float | None, str | None]]:
        specs = [
            ReadAccessSpecification(
                objectIdentifier=ObjectIdentifier(obj_id),
                listOfPropertyReferences=[
                    PropertyReference(propertyIdentifier=PropertyIdentifier(prop_id))
                ],
            )
            for obj_id, prop_id in requests
        ]

        result_list = await self._app.read_property_multiple(Address(address), specs)

        out = []
        for obj_id, prop_id in requests:
            val = None
            status = None
            if result_list:
                for item in result_list:
                    if str(item.objectIdentifier) == obj_id:
                        for result in item.listOfResults:
                            if str(result.propertyIdentifier) == prop_id:
                                pv = result.propertyValue
                                if isinstance(pv, AnyAtomic):
                                    val = _to_float(pv.cast_out(None))
                                elif hasattr(pv, "errorCode"):
                                    status = str(pv.errorCode)
                                break
                        break  # found the matching object; stop searching
            out.append((obj_id, val, status))
        return out

    async def subscribe_cov(
        self,
        address: str,
        device_id: int,
        obj_id: str,
        callback: Callable[[str, float, str], Awaitable[None]],
        lifetime: int = 300,
    ) -> None:
        key = (address, obj_id)
        if key in self._cov_tasks and not self._cov_tasks[key].done():
            return  # already subscribed and still running

        task = asyncio.create_task(
            self._cov_loop(address, obj_id, callback, lifetime),
            name=f"bacpypes3-cov-{obj_id}",
        )
        self._cov_tasks[key] = task

    async def _cov_loop(
        self,
        address: str,
        obj_id: str,
        callback: Callable[[str, float, str], Awaitable[None]],
        lifetime: int,
    ) -> None:
        """Maintain a COV subscription, renewing before it expires."""
        addr = Address(address)
        oid = ObjectIdentifier(obj_id)

        while True:
            try:
                # bacpypes3 provides an async context manager for COV subscriptions.
                async with self._app.change_of_value(addr, oid, lifetime=lifetime) as cov:
                    async for notification in cov:
                        value = _to_float(notification.presentValue)
                        status = str(notification.statusFlags) if notification.statusFlags else None
                        if value is not None:
                            await callback(obj_id, value, status or "")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("bacnet: COV loop error for %s: %s — retrying", obj_id, exc)
                await asyncio.sleep(5)

    async def write_property(
        self,
        address: str,
        device_id: int,
        obj_id: str,
        prop_id: str,
        value: float,
        priority: int,
    ) -> None:
        await self._app.write_property(
            Address(address),
            ObjectIdentifier(obj_id),
            PropertyIdentifier(prop_id),
            Real(value),
            priority=priority,
        )

    async def close(self) -> None:
        for task in self._cov_tasks.values():
            task.cancel()
        if self._cov_tasks:
            await asyncio.gather(*self._cov_tasks.values(), return_exceptions=True)
        self._cov_tasks.clear()
        self._app.close()
