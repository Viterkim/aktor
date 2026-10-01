#![no_std]

extern crate alloc;

use aktor::*;
use alloc::{rc::Rc, vec::Vec};
use core::cell::RefCell;
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use embassy_time::Timer;
use er::*;

#[cfg(all(test, not(target_os = "none")))]
mod tests;

pub struct Sensor {
    pub readings: Rc<RefCell<Vec<u32>>>,
}

#[derive(Er)]
#[er(no_constructors)]
pub struct SensorError {
    pub value: u32,
}

#[aktor]
pub async fn record(sensor: &mut Sensor, value: u32) -> ErResult<usize, SensorError> {
    if value == 0 {
        return Err(SensorError { value }.into());
    }

    sensor.readings.borrow_mut().push(value);

    Ok(sensor.readings.borrow().len())
}

#[aktor]
pub async fn record_many<I: IntoIterator<Item = u32>>(
    sensor: &mut Sensor,
    values: I,
) -> ErResult<usize, SensorError> {
    for value in values {
        record(&mut *sensor, value).await?;
    }

    Ok(sensor.readings.borrow().len())
}

#[aktor]
pub async fn hold(
    sensor: &mut Sensor,
    started: Rc<Signal<NoopRawMutex, ()>>,
    release: Rc<Signal<NoopRawMutex, ()>>,
) -> ErResult<usize, SensorError> {
    let readings = sensor.readings.clone();
    started.signal(());
    release.wait().await;
    Timer::after_millis(1).await;

    record(sensor, 1).await?;
    let count = readings.borrow().len();

    Ok(count)
}

#[embassy_executor::task]
pub async fn sensor_owner(owner: embassy::Owner<Sensor, 2, &'static str>) {
    let _result = owner
        .run_with(
            async || {
                Timer::after_millis(1).await;

                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },
            async |sensor| {
                Timer::after_millis(1).await;
                drop(sensor);

                Ok(())
            },
        )
        .await;
}
