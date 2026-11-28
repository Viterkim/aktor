#![no_std]

#[cfg(test)]
extern crate std;

extern crate alloc;

use aktor::*;
use alloc::{rc::Rc, string::String, vec::Vec};
use core::cell::RefCell;
use core::num::NonZeroUsize;
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use embassy_time::Timer;
use er::*;

#[cfg(all(test, not(target_os = "none")))]
mod tests;

pub struct Sensor {
    pub readings: Rc<RefCell<Vec<u32>>>,
}

#[derive(Er)]
pub struct SensorError {
    pub value: u32,
}

#[aktor(latest)]
pub async fn record(sensor: &mut Sensor, value: u32) -> ErResult<usize, SensorError> {
    if value == 0 {
        er_bail!(value);
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

#[aktor(latest)]
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
pub async fn sensor_owner(owner: embassy::Owner<Sensor, &'static str>) {
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

#[aktor::aktor(latest)]
pub async fn local_search(_: &Sensor, text: impl AsRef<str>) -> alloc::string::String {
    text.as_ref().into()
}

#[embassy_executor::task(pool_size = 2)]
async fn setup_driver(driver: aktor::message::LocalFuture<'static, ()>) {
    driver.await;
}

#[derive(Er)]
struct SpawnDriverErr;

pub fn sensor_setup(
    spawner: embassy_executor::Spawner,
) -> impl aktor::embassy::AktorDefinition<aktor::embassy::Handle<Sensor, ()>> {
    AktorNew {
        name: "sensor".into(),
        kind: AktorKind::EmbassyLocal(move |future| {
            let task = setup_driver(future).er(())?;

            spawner.spawn(task);
            Ok::<(), (SpawnDriverErr, String)>(())
        }),
        start: AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    }
}

pub fn shared_sensor_setup(
    spawner: embassy_executor::Spawner,
) -> impl aktor::embassy::AktorDefinition<aktor::cross_core::Handle<Sensor>> {
    AktorNew {
        name: "shared sensor".into(),
        kind: AktorKind::EmbassyCrossCore(move |future| {
            let task = setup_driver(future).er(())?;

            spawner.spawn(task);
            Ok::<(), (SpawnDriverErr, String)>(())
        }),
        start: AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    }
}
