use aktor::{AktorCleanupError, AktorData, AktorSetupError, aktor};

pub const WIRE_REVISION: &str = "power-v2";

pub struct PowerRemote;

pub struct PowerManager {
    cycles: u32,
    target: i32,
}
impl PowerManager {
    pub fn open() -> Result<Self, AktorSetupError> {
        Ok(Self {
            cycles: 0,
            target: 0,
        })
    }

    pub fn close(self) -> Result<(), AktorCleanupError> {
        println!("power manager closed after {} cycles", self.cycles);
        Ok(())
    }
}

#[derive(AktorData, Debug)]
pub struct PowerState {
    pub cycles: u32,
    pub target: i32,
}

#[derive(AktorData, Debug)]
pub enum PowerError {
    InvalidDuration,
}

#[aktor(remote, role = PowerRemote)]
pub async fn get_state(power: &PowerManager) -> PowerState {
    PowerState {
        cycles: power.cycles,
        target: power.target,
    }
}

#[aktor(remote, role = PowerRemote)]
pub async fn set_target(power: &mut PowerManager, target: i32) -> i32 {
    power.target = target;
    target
}

#[aktor(remote, role = PowerRemote)]
pub async fn power_cycle_gnss(power: &mut PowerManager, seconds: u32) -> Result<u32, PowerError> {
    if seconds == 0 {
        return Err(PowerError::InvalidDuration);
    }

    power.cycles += 1;
    Ok(power.cycles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue() {
        let operations = aktor::worker::Operations::for_remote::<PowerManager, PowerRemote>();
        let names: Vec<_> = operations
            .0
            .iter()
            .map(|operation| operation.split_once(": ").unwrap().0)
            .collect();

        assert_eq!(
            names,
            [
                "power_api::get_state",
                "power_api::power_cycle_gnss",
                "power_api::set_target"
            ]
        );
    }
}
