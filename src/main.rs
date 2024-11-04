use std::time::{Duration, Instant};
use capnp::serialize;
use tmq::Context;
use anyhow::Result;
use tokio::sync::mpsc::{self, Receiver};
use futures::StreamExt;
use rumqttc::{AsyncClient, MqttOptions, QoS, TlsConfiguration, Transport};
use serde::Serialize;
use crate::obd_data::Process;

mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
mod car_capnp {
    include!(concat!(env!("OUT_DIR"), "/car_capnp.rs"));
}
mod custom_capnp {
    include!(concat!(env!("OUT_DIR"), "/custom_capnp.rs"));
}
mod legacy_capnp {
    include!(concat!(env!("OUT_DIR"), "/legacy_capnp.rs"));
}

mod obd_data {
    use serde::{Deserialize, Serialize};

    pub trait Process {
        fn process(data: &[u8]) -> Option<Data>;
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum ChargingType {
        NotCharging,
        AC,
        DC,
        Other,
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Battery01 {
        pub charging: ChargingType,
        pub bms_ignition: bool,
        pub bms_relay: bool,
        pub aux_battery_voltage: f32,

        pub bms_soc: f32,
        pub battery_current: f32,
        pub battery_voltage: f32,
        pub battery_power: f32,

        pub fan_status: u8,
        pub fan_speed: u8,

        pub cumulative_energy_charged: f64,
        pub cumulative_energy_discharged: f64,
        pub cumulative_charge_current: f64,
        pub cumulative_discharge_current: f64,

        pub cumulative_operating_time: u32,

        //pub available_charge_power: f32,
        //pub available_discharge_power: f32,

        pub dc_battery_inlet_temp: i8,
        pub dc_battery_max_temp: i8,
        pub dc_battery_min_temp: i8,
        pub dc_battery_cell_max_voltage: f32,
        pub dc_battery_cell_max_voltage_id: u8,
        pub dc_battery_cell_min_voltage: f32,
        pub dc_battery_cell_min_voltage_id: u8,

        pub inverter_capacitor_voltage: u16,
        pub isolation_resistance: u16,

        pub rear_drive_motor_speed: i16,
        pub front_drive_motor_speed: i16,
        pub dc_battery_module_temp1: i8,
        pub dc_battery_module_temp2: i8,
        pub dc_battery_module_temp3: i8,
        pub dc_battery_module_temp4: i8,
        pub dc_battery_module_temp5: i8,
    }
    impl Process for Battery01 {
        fn process(data: &[u8]) -> Option<Data> {
            let mut data = Self {
                // Car Scanner says charging flag is data[9]
                // Own analysis suggests maybe it's data[5]
                charging:
                    if data[5] & 0x20 > 0 { ChargingType::AC }
                    else if data[5] & 0x40 > 0 { ChargingType::DC }
                    else if data[5] & 0x80 > 0 { ChargingType::Other }
                    else { ChargingType::NotCharging },
                //available_discharge_power: u16::from_be_bytes(data[5..7].try_into().unwrap()) as f32 / 100.0,
                //available_charge_power: u16::from_be_bytes(data[7..9].try_into().unwrap()) as f32 / 100.0,

                bms_ignition: data[50] & 0x04 > 0,
                bms_relay: data[9] & 0x01 > 0,
                bms_soc: data[4] as f32 / 2.0,
                battery_current: i16::from_be_bytes(data[10..12].try_into().unwrap()) as f32 / 10.0,
                battery_voltage: u16::from_be_bytes(data[12..14].try_into().unwrap()) as f32 / 10.0,
                battery_power: 0.0,
                aux_battery_voltage: data[29] as f32 / 10.0,
                fan_status: data[27],
                fan_speed: data[28],
                cumulative_energy_charged: u32::from_be_bytes(data[38..42].try_into().unwrap()) as f64 / 10.0,
                cumulative_energy_discharged: u32::from_be_bytes(data[42..46].try_into().unwrap()) as f64 / 10.0,
                cumulative_charge_current: u32::from_be_bytes(data[30..34].try_into().unwrap()) as f64 / 10.0,
                cumulative_discharge_current: u32::from_be_bytes(data[34..38].try_into().unwrap()) as f64 / 10.0,
                cumulative_operating_time: u32::from_be_bytes(data[46..50].try_into().unwrap()),
                dc_battery_inlet_temp: data[22] as i8 - 40,
                dc_battery_max_temp: data[14] as i8,
                dc_battery_min_temp: data[15] as i8,
                dc_battery_cell_max_voltage: data[23] as f32 / 50.0,
                dc_battery_cell_max_voltage_id: data[24],
                dc_battery_cell_min_voltage: data[25] as f32 / 50.0,
                dc_battery_cell_min_voltage_id: data[26],
                rear_drive_motor_speed: i16::from_be_bytes(data[53..55].try_into().unwrap()),
                front_drive_motor_speed: i16::from_be_bytes(data[55..57].try_into().unwrap()),
                dc_battery_module_temp1: data[16] as i8,
                dc_battery_module_temp2: data[17] as i8,
                dc_battery_module_temp3: data[18] as i8,
                dc_battery_module_temp4: data[19] as i8,
                dc_battery_module_temp5: data[20].try_into().unwrap(),
                inverter_capacitor_voltage: u16::from_be_bytes(data[51..53].try_into().unwrap()),
                isolation_resistance: u16::from_be_bytes(data[57..59].try_into().unwrap()),
            };
            data.battery_power = data.battery_voltage * data.battery_current / 1000.0;
            if data.inverter_capacitor_voltage == 6553 {
                data.inverter_capacitor_voltage = 0;
            }
            if data.aux_battery_voltage == 0.0 || data.dc_battery_inlet_temp == -40 || data.dc_battery_inlet_temp == 127 - 40 {
                return None;
            }
            Some(Data::Battery01(data))
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Battery05 {
        pub available_charge_power: f32,
        pub available_discharge_power: f32,
        pub battery_cell_voltage_deviation: f32,
        pub battery_heater_1_temp: i8,
        pub battery_heater_2_temp: i8,

        pub remaining_energy: f32, // in Wh
        pub soc: f32,
        pub soh: f32,

        pub dc_battery_module_temp6: i8,
        pub dc_battery_module_temp7: i8,
        pub dc_battery_module_temp8: i8,
        pub dc_battery_module_temp9: i8,
        pub dc_battery_module_temp10: i8,
        pub dc_battery_module_temp11: i8,
        pub dc_battery_module_temp12: i8,
        pub dc_battery_module_temp13: i8,
        pub dc_battery_module_temp14: i8,
        pub dc_battery_module_temp15: i8,
        pub dc_battery_module_temp16: i8,
    }
    impl Process for Battery05 {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                available_charge_power: u16::from_be_bytes(data[16..18].try_into().unwrap()) as f32 / 100.0,
                available_discharge_power: u16::from_be_bytes(data[18..20].try_into().unwrap()) as f32 / 100.0,
                battery_cell_voltage_deviation: data[20] as f32 / 50.0,
                battery_heater_1_temp: data[23] as i8,
                battery_heater_2_temp: data[24] as i8,
                remaining_energy: u16::from_be_bytes(data[28..30].try_into().unwrap()) as f32 * 2.0,
                soc: data[31] as f32 / 2.0,
                soh: u16::from_be_bytes(data[25..27].try_into().unwrap()) as f32 / 10.0,
                dc_battery_module_temp6: data[9] as i8,
                dc_battery_module_temp7: data[10] as i8,
                dc_battery_module_temp8: data[11] as i8,
                dc_battery_module_temp9: data[12] as i8,
                dc_battery_module_temp10: data[13] as i8,
                dc_battery_module_temp11: data[14] as i8,
                dc_battery_module_temp12: data[15] as i8,
                dc_battery_module_temp13: data[39] as i8,
                dc_battery_module_temp14: data[40] as i8,
                dc_battery_module_temp15: data[41] as i8,
                dc_battery_module_temp16: data[42] as i8,
            };
            if data.remaining_energy == 131070.0 || data.remaining_energy == 0.0 {
                return None;
            }
            if data.soc == 0.0 || data.soh == 0.0 {
                return None;
            }
            Some(Data::Battery05(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Battery11 {
        pub ac_charging_events: u32,
        pub dc_charging_events: u32,
        pub cumulative_ac_charging_energy: u32,
        pub cumulative_dc_charging_energy: u32,

        pub dc_battery_module_temp17: i8,
        pub dc_battery_module_temp18: i8,
    }
    impl Process for Battery11 {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::Battery11(Self {
                ac_charging_events: u32::from_be_bytes(data[4..8].try_into().unwrap()),
                dc_charging_events: u32::from_be_bytes(data[8..12].try_into().unwrap()),
                cumulative_ac_charging_energy: u32::from_be_bytes(data[12..16].try_into().unwrap()),
                cumulative_dc_charging_energy: u32::from_be_bytes(data[16..20].try_into().unwrap()),
                dc_battery_module_temp17: data[26] as i8,
                dc_battery_module_temp18: data[27] as i8,
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct TirePressures {
        pub front_left_psi: f32,
        pub front_left_temp: u8,
        pub front_right_psi: f32,
        pub front_right_temp: u8,
        pub rear_left_psi: f32,
        pub rear_left_temp: u8,
        pub rear_right_psi: f32,
        pub rear_right_temp: u8,
    }
    impl Process for TirePressures {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                front_left_psi: data[4] as f32 / 5.0,
                front_left_temp: data[5] - 55,
                front_right_psi: data[9] as f32 / 5.0,
                front_right_temp: data[10] - 55,
                rear_left_psi: data[14] as f32 / 5.0,
                rear_left_temp: data[15] - 55,
                rear_right_psi: data[19] as f32 / 5.0,
                rear_right_temp: data[20] - 55,
            };
            if data.front_right_psi == 0.0 || data.front_left_psi == 0.0 || data.rear_left_psi == 0.0 || data.rear_right_psi == 0.0 {
                return None;
            }
            Some(Data::TirePressures(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct HVAC {
        pub indoor_temp: f32,
        pub outdoor_temp: f32,
        pub vehicle_speed: u8,
    }
    impl Process for HVAC {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::HVAC(Self {
                indoor_temp: data[5] as f32 / 2.0 - 40.0,
                outdoor_temp: data[6] as f32 / 2.0 - 40.0,
                vehicle_speed: data[29],
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct ICCU01 {
        ac_maximum_current_limit: f32,
        dc_maximum_current_limit: f32,
        dc_target_voltage: f32,
        v2l_ac_target_voltage: f32,
        v2l_ac_current_limit: f32,
        v2l_dc_current_limit: f32,
    }
    impl Process for ICCU01 {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::ICCU01(Self {
                ac_maximum_current_limit: u16::from_be_bytes(data[12..14].try_into().unwrap()) as f32 / 10.0,
                dc_maximum_current_limit: u16::from_be_bytes(data[10..12].try_into().unwrap()) as f32 / 10.0,
                dc_target_voltage: u16::from_be_bytes(data[8..10].try_into().unwrap()) as f32 / 10.0,
                v2l_ac_target_voltage: u16::from_be_bytes(data[14..16].try_into().unwrap()) as f32 / 10.0,
                v2l_ac_current_limit: data[17] as f32,
                v2l_dc_current_limit: data[16] as f32,
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct ICCU02 {
        obc_ac_current_a: f32,
        obc_ac_current_b: f32,
        obc_ac_frequency: f32,
        obc_ac_total_current: f32,
        obc_ac_voltage_a: f32,
        obc_ac_voltage_b: f32,
        obc_ac_power: f32,

        obc_dc_current_a: f32,
        obc_dc_current_b: f32,
        obc_dc_total_current: f32,
        obc_dc_voltage: f32,
        obc_dc_power: f32,
        obc_charging_loss: f32,

        obc_temp_a: i8,
        obc_temp_b: i8,
    }
    impl Process for ICCU02 {
        fn process(data: &[u8]) -> Option<Data> {
            let mut data = Self {
                obc_ac_current_a: i16::from_be_bytes(data[13..15].try_into().unwrap()) as f32 / 100.0,
                obc_ac_current_b: i16::from_be_bytes(data[15..17].try_into().unwrap()) as f32 / 100.0,
                obc_ac_frequency: data[10] as f32,
                obc_ac_total_current: i16::from_be_bytes(data[11..13].try_into().unwrap()) as f32 / 100.0,
                obc_ac_voltage_a: u16::from_be_bytes(data[4..6].try_into().unwrap()) as f32 / 10.0,
                obc_ac_voltage_b: u16::from_be_bytes(data[6..8].try_into().unwrap()) as f32 / 10.0,
                obc_ac_power: 0.0,

                obc_dc_current_a: i16::from_be_bytes(data[23..25].try_into().unwrap()) as f32 / 100.0,
                obc_dc_current_b: i16::from_be_bytes(data[25..27].try_into().unwrap()) as f32 / 100.0,
                obc_dc_total_current: i16::from_be_bytes(data[21..23].try_into().unwrap()) as f32 / 100.0,
                obc_dc_voltage: u16::from_be_bytes(data[19..21].try_into().unwrap()) as f32 / 10.0,
                obc_dc_power: 0.0,
                obc_charging_loss: 0.0,

                obc_temp_a: data[27] as i8 - 40,
                obc_temp_b: data[28] as i8 - 40,
            };
            data.obc_ac_power = data.obc_ac_total_current * data.obc_ac_voltage_a / 1000.0;
            data.obc_dc_power = data.obc_dc_total_current * data.obc_dc_voltage / 1000.0;
            data.obc_charging_loss = data.obc_ac_power - data.obc_dc_power;
            Some(Data::ICCU02(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct ICCU03 {
        obc_dc_target_current: f32,
        obc_dc_target_voltage: f32,
    }
    impl Process for ICCU03 {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::ICCU03(Self {
                obc_dc_target_current: u16::from_be_bytes(data[10..12].try_into().unwrap()) as f32 / 10.0,
                obc_dc_target_voltage: u16::from_be_bytes(data[8..10].try_into().unwrap()) as f32 / 10.0,
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct ICCU11 {
        aux_battery_current: f32,
        aux_battery_soc: u8,
        aux_battery_temp: f32,
        aux_battery_voltage_iccu: f32,
        ldc_input_voltage: f32,
        ldc_output_current: f32,
        ldc_output_voltage: f32,
        ldc_output_power: f32,
        ldc_temp: i8,
    }
    impl Process for ICCU11 {
        fn process(data: &[u8]) -> Option<Data> {
            let mut data = Self {
                aux_battery_current: u16::from_be_bytes(data[18..20].try_into().unwrap()) as f32 / 100.0 - 327.0,
                aux_battery_soc: data[20],
                aux_battery_temp: u16::from_be_bytes(data[23..25].try_into().unwrap()) as f32 / 2.0 - 40.0,
                aux_battery_voltage_iccu: u16::from_be_bytes(data[21..23].try_into().unwrap()) as f32 / 1000.0 + 6.0,
                ldc_input_voltage: u16::from_be_bytes(data[14..16].try_into().unwrap()) as f32 / 10.0,
                ldc_output_current: u16::from_be_bytes(data[12..14].try_into().unwrap()) as f32 / 1000.0,
                ldc_output_voltage: u16::from_be_bytes(data[10..12].try_into().unwrap()) as f32 / 1000.0,
                ldc_output_power: 0.0,
                ldc_temp: data[9] as i8 - 100,
            };
            data.ldc_output_power = data.ldc_output_current * data.ldc_output_voltage;
            Some(Data::ICCU11(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct VCMS01 {
        ac_charging_state: bool, // [17] & bit 2
        dc_charging_state: bool, // [17] & bit 3
        aux_battery_voltage_vcms: f32, // [11..13] / 100.0
        estimated_charging_power: f32, // [28..30] * 10.0 - 50.0
        v2l_discharging_current: f32, // [27]
    }
    impl Process for VCMS01 {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                ac_charging_state: data[17] & (1 << 2) != 0,
                dc_charging_state: data[17] & (1 << 3) != 0,
                aux_battery_voltage_vcms: u16::from_be_bytes(data[11..13].try_into().unwrap()) as f32 / 100.0,
                estimated_charging_power: u16::from_be_bytes(data[28..30].try_into().unwrap()) as f32 * 10.0 - 50.0,
                v2l_discharging_current: data[27] as f32,
            };
            Some(Data::VCMS01(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct VCMS02 {
        evse_delivered_power: f32, // [23..25] * 10.0
        evse_main_power: f32, // [27]
        evse_max_voltage: f32, // [9..11] / 10.0
        evse_max_current: f32, // [11..13] / 10.0
        evse_min_voltage: f32, // [13..15] / 10.0
        evse_min_current: f32, // [15..17] / 10.0
        evse_output_voltage: f32, // [17..19] / 10.0
        evse_output_current: f32, // [19..21] / 10.0
        evse_max_power: f32, // [25..27] * 10.0
    }
    impl Process for VCMS02 {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                evse_delivered_power: u16::from_be_bytes(data[23..25].try_into().unwrap()) as f32 * 10.0,
                evse_main_power: data[27] as f32,
                evse_max_voltage: u16::from_be_bytes(data[9..11].try_into().unwrap()) as f32 / 10.0,
                evse_max_current: u16::from_be_bytes(data[11..13].try_into().unwrap()) as f32 / 10.0,
                evse_min_voltage: u16::from_be_bytes(data[13..15].try_into().unwrap()) as f32 / 10.0,
                evse_min_current: u16::from_be_bytes(data[15..17].try_into().unwrap()) as f32 / 10.0,
                evse_output_voltage: u16::from_be_bytes(data[17..19].try_into().unwrap()) as f32 / 10.0,
                evse_output_current: u16::from_be_bytes(data[19..21].try_into().unwrap()) as f32 / 10.0,
                evse_max_power: u16::from_be_bytes(data[25..27].try_into().unwrap()) as f32 * 10.0,
            };
            Some(Data::VCMS02(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct VCMS03 {
        ac_charger_current: f32, // [38..40] / 100.0
        ac_charging_counter: u16, // [30..32]
        ac_charging_time: u16, // [34..36]
        ac_charging_time_after_plugin: u16, // [42..44]
        bms_current_target: f32, // [26..28] / 10.0
        bms_voltage_target: f32, // [24..26] / 10.0
        cp_duty_cycle: f32, // [7..9] / 10.0
        cp_voltage: f32, // [6] / 10.0
        dc_charging_time: u16, // [36..38]
        dc_charging_time_after_plugin: u16, // [44..46]
        dc_max_current: f32, // [40..42] / 100.0
        dc_charging_counter: u16, // [32..34]
        ev_max_current: f32, // [22..24] / 10.0
        ev_max_voltage: f32, // [20..22] / 10.0
        evse_target_voltage: f32, // [16..18] / 10.0
        evse_target_current: f32, // [18..20] / 10.0
        hv_battery_soc: f32, // [15] / 2.0
        hv_battery_voltage: f32, // [13..15] / 10.0
    }
    impl Process for VCMS03 {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                ac_charger_current: u16::from_be_bytes(data[38..40].try_into().unwrap()) as f32 / 100.0,
                ac_charging_counter: u16::from_be_bytes(data[30..32].try_into().unwrap()),
                ac_charging_time: u16::from_be_bytes(data[34..36].try_into().unwrap()),
                ac_charging_time_after_plugin: u16::from_be_bytes(data[42..44].try_into().unwrap()),
                bms_current_target: u16::from_be_bytes(data[26..28].try_into().unwrap()) as f32 / 10.0,
                bms_voltage_target: u16::from_be_bytes(data[24..26].try_into().unwrap()) as f32 / 10.0,
                cp_duty_cycle: u16::from_be_bytes(data[7..9].try_into().unwrap()) as f32 / 10.0,
                cp_voltage: data[6] as f32 / 10.0,
                dc_charging_time: u16::from_be_bytes(data[36..38].try_into().unwrap()),
                dc_charging_time_after_plugin: u16::from_be_bytes(data[44..46].try_into().unwrap()),
                dc_max_current: u16::from_be_bytes(data[40..42].try_into().unwrap()) as f32 / 100.0,
                dc_charging_counter: u16::from_be_bytes(data[32..34].try_into().unwrap()),
                ev_max_current: u16::from_be_bytes(data[22..24].try_into().unwrap()) as f32 / 10.0,
                ev_max_voltage: u16::from_be_bytes(data[20..22].try_into().unwrap()) as f32 / 10.0,
                evse_target_voltage: u16::from_be_bytes(data[16..18].try_into().unwrap()) as f32 / 10.0,
                evse_target_current: u16::from_be_bytes(data[18..20].try_into().unwrap()) as f32 / 10.0,
                hv_battery_soc: data[15] as f32 / 2.0,
                hv_battery_voltage: u16::from_be_bytes(data[13..15].try_into().unwrap()) as f32 / 10.0,
            };
            Some(Data::VCMS03(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct VCMS04 {
        ac_inlet_1_temperature: i8, // [10] - 50
        main_battery_relay_status: bool, // [4] & bit 2
        charging_socket_connected: bool, // [4] & bit 1
        dc_inlet_1_temperature: i8, // [8] - 50,
        dc_inlet_2_temperature: i8, // [9] - 50,
    }
    impl Process for VCMS04 {
        fn process(data: &[u8]) -> Option<Data> {
            let data = Self {
                ac_inlet_1_temperature: data[10] as i8 - 50,
                main_battery_relay_status: data[4] & (1 << 2) != 0,
                charging_socket_connected: data[4] & (1 << 1) != 0,
                dc_inlet_1_temperature: data[8] as i8 - 50,
                dc_inlet_2_temperature: data[9] as i8 - 50,
            };
            Some(Data::VCMS04(data))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Dashboard {
        pub odometer: u32,
    }
    impl Process for Dashboard {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::Dashboard(Self {
                odometer: ((data[9] as u32) << 16) + ((data[10] as u32) << 8) + (data[11] as u32),
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct IGPM03 {
        ignition_on: bool,
        driver_seat_belt: bool,
        passenger_seat_belt: bool,
        hood_open: bool,
        rear_right_door_open: bool,
        rear_right_door_unlocked: bool,
        rear_left_door_open: bool,
        rear_left_door_unlocked: bool,
        driver_door_open: bool,
        passenger_door_open: bool,
        trunk_open: bool,
    }
    impl Process for IGPM03 {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::IGPM03(Self {
                ignition_on: data[5] & 0x60 > 0,
                driver_seat_belt: data[5] & (1 << 1) > 0,
                passenger_seat_belt: data[5] & (1 << 2) > 0,
                hood_open: data[5] & (1 << 0) > 0,
                rear_left_door_open: data[4] & (1 << 0) > 0,
                rear_left_door_unlocked: data[4] & (1 << 1) > 0,
                rear_right_door_open: data[4] & (1 << 2) > 0,
                rear_right_door_unlocked: data[4] & (1 << 3) > 0,
                passenger_door_open: data[4] & (1 << 4) > 0,
                driver_door_open: data[4] & (1 << 5) > 0,
                trunk_open: data[4] & (1 << 7) > 0,
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct IGPM04 {
        driver_door_unlocked: bool,
        passenger_door_unlocked: bool,
        rear_left_seat_belt: bool,
        rear_center_seat_belt: bool,
        rear_right_seat_belt: bool,
    }
    impl Process for IGPM04 {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::IGPM04(Self {
                driver_door_unlocked: data[4] & (1 << 3) > 0,
                passenger_door_unlocked: data[4] & (1 << 2) > 0,
                rear_left_seat_belt: data[6] & (1 << 2) > 0,
                rear_center_seat_belt: data[6] & (1 << 3) > 0,
                rear_right_seat_belt: data[6] & (1 << 4) > 0,
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct CabinEnvironment {
        pub pressure: f32,
        pub temperature: f32,
        pub humidity: f32,
    }
    impl Process for CabinEnvironment {
        fn process(data: &[u8]) -> Option<Data> {
            Some(Data::CabinEnvironment(Self {
                pressure: f32::from_be_bytes(data[0..4].try_into().unwrap()),
                temperature: f32::from_be_bytes(data[4..8].try_into().unwrap()),
                humidity: f32::from_be_bytes(data[8..12].try_into().unwrap()),
            }))
        }
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub enum Gear {
        Park,
        Reverse,
        Neutral,
        Drive
    }
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Shifter {
        pub gear: Gear,
    }
    impl Process for Shifter {
        fn process(data: &[u8]) -> Option<Data> {
            let gear = match data[8] {
                1 => Gear::Park,
                2 => Gear::Reverse,
                3 => Gear::Neutral,
                4 => Gear::Drive,
                _ => return None,
            };
            Some(Data::Shifter(Self {
                gear,
            }))
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Data {
        Battery01(Battery01),
        Battery05(Battery05),
        Battery11(Battery11),
        TirePressures(TirePressures),
        HVAC(HVAC),
        ICCU01(ICCU01),
        ICCU02(ICCU02),
        ICCU03(ICCU03),
        ICCU11(ICCU11),
        VCMS01(VCMS01),
        VCMS02(VCMS02),
        VCMS03(VCMS03),
        VCMS04(VCMS04),
        Dashboard(Dashboard),
        IGPM03(IGPM03),
        IGPM04(IGPM04),
        CabinEnvironment(CabinEnvironment),
        Shifter(Shifter),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = mpsc::channel::<(obd_data::Data, String)>(10);

    tokio::spawn(mqtt(rx));

    let mut socket = tmq::subscribe(&Context::new())
        .connect("tcp://127.0.0.1:7015")? // Port for the CAN stream
        .subscribe(&[])?;

    let mut last_shifter_message = Instant::now();

    while let Some(messages) = socket.next().await {
        for message in messages? {
            let message_reader = serialize::read_message(
                &*message,
                capnp::message::ReaderOptions::new(),
            )?;
            let event = message_reader.get_root::<log_capnp::event::Reader>()?;
            match event.which()? {
                log_capnp::event::Can(Ok(can_data)) => {
                    for can_event in can_data.iter() {
                        let data = can_event.get_dat()?;
                        if can_event.get_src() == 0 && (0x700..=0x7A0).contains(&can_event.get_address()) {
                            // TODO: for debug logging only
                            if can_event.get_address() == 0x701 && data[4] == 0x00
                            || can_event.get_address() == 0x710 && data[5] < 55 {
                                eprintln!("Possible all-blank data example: {:x} -> {:02x?}", can_event.get_address(), &data);
                            }
                            if data.iter().all(|x| x == &0x00) {
                                continue;
                            }
                            let raw = format!("{:02x?}", &data);
                            let processed_data = match can_event.get_address() {
                                0x700 => {
                                    eprintln!("Forwarded error: {}", String::from_utf8(data.to_vec())?);
                                    continue;
                                },
                                0x701 => obd_data::Battery01::process(data),
                                0x705 => obd_data::Battery05::process(data),
                                0x706 => continue, // No important data yet
                                0x70B => obd_data::Battery11::process(data),
                                0x710 => obd_data::TirePressures::process(data),
                                0x720 => obd_data::HVAC::process(data),
                                0x730 => continue, // TODO: ADAS for accelerations, steering angle, wheel speeds
                                0x741 => obd_data::ICCU01::process(data),
                                0x742 => obd_data::ICCU02::process(data),
                                0x743 => obd_data::ICCU03::process(data),
                                0x74B => obd_data::ICCU11::process(data),
                                0x751 => obd_data::VCMS01::process(data),
                                0x752 => obd_data::VCMS02::process(data),
                                0x753 => obd_data::VCMS03::process(data),
                                0x754 => obd_data::VCMS04::process(data),
                                0x760 => obd_data::Dashboard::process(data),
                                0x773 => obd_data::IGPM03::process(data),
                                0x774 => obd_data::IGPM04::process(data),
                                0x7A0 => obd_data::CabinEnvironment::process(data),
                                _ => {
                                    println!("Unknown forwarding address 0x{:x}", can_event.get_address());
                                    continue;
                                },
                            };
                            if let Some(processed_data) = processed_data {
                                tx.send((processed_data, raw)).await?;
                            }
                        }
                        else if can_event.get_src() == 1 && can_event.get_address() == 0x130 {
                            if last_shifter_message.elapsed().as_millis() > 1000 {
                                // Gear shifter message
                                let processed_data = obd_data::Shifter::process(data);
                                if let Some(processed_data) = processed_data {
                                    tx.send((processed_data, String::new())).await?;
                                }
                                last_shifter_message = Instant::now();
                            }
                        }
                    }
                },
                _ => {},
            }
        }
    }
    Ok(())
}

#[derive(Serialize, Debug)]
struct HASSDevice<'a> {
    identifiers: &'a str,
    manufacturer: &'a str,
    model: &'a str,
    name: &'a str,
    serial_number: &'a str,
    sw_version: &'a str,
}
const IONIQ_5: HASSDevice = HASSDevice {
    manufacturer: "Hyundai",
    model: "Ioniq 5",
    name: "Ioniq 5",
    serial_number: "KM8KRDDF1RU297513",
    identifiers: "KM8KRDDF1RU297513",
    sw_version: "0.1.0",
};
#[derive(Serialize, Debug)]
struct HASSSensor<'a> {
    #[serde(skip)]
    id: &'a str,
    device: HASSDevice<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_after: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_attributes_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_attributes_topic: Option<String>,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggested_display_precision: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_class: Option<String>,
    state_topic: String,
    unique_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit_of_measurement: Option<String>,
    value_template: String,
}
impl<'a> HASSSensor<'a> {
    pub fn new(name: &'a str, id: &'a str, device_class: &'a str, topic_root: &'a str) -> Self {
        Self {
            id,
            device: IONIQ_5,
            name: name.into(),
            unique_id: format!("ioniq_{}", id),
            device_class: if device_class == "" { None } else { Some(device_class.into()) },
            state_topic: format!("homeassistant/sensor/{}/state", topic_root),
            json_attributes_topic: Some(format!("homeassistant/sensor/{}/raw", topic_root)),
            value_template: format!("{{{{ value_json.{} }}}}", id),

            expire_after: Some(60 * 60), // 1 hour
            icon: None,
            json_attributes_template: None,
            suggested_display_precision: None,
            state_class: None, //Some("measurement".into()),
            unit_of_measurement: None,
        }
    }
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit_of_measurement = Some(unit.into());
        self
    }
    pub fn dont_expire(mut self) -> Self {
        self.expire_after = None;
        self
    }

    pub fn config_topic(&self) -> String {
        let mut topic_components: Vec<_> = self.state_topic.split("/").collect();
        topic_components.pop();
        topic_components.pop();
        topic_components.push(self.id);
        topic_components.push("config");
        topic_components.join("/")
    }
}
#[derive(Serialize, Debug)]
struct HASSBinarySensor<'a> {
    #[serde(skip)]
    id: &'a str,
    device: HASSDevice<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_after: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_attributes_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_attributes_topic: Option<String>,
    name: String,
    state_topic: String,
    unique_id: String,
    payload_on: bool,
    payload_off: bool,
    value_template: String,
}
impl<'a> HASSBinarySensor<'a> {
    pub fn new(name: &'a str, id: &'a str, device_class: &'a str, topic_root: &'a str) -> Self {
        Self {
            id,
            device: IONIQ_5,
            name: name.into(),
            unique_id: format!("ioniq_{}", id),
            device_class: if device_class == "" { None } else { Some(device_class.into()) },
            state_topic: format!("homeassistant/sensor/{}/state", topic_root),
            json_attributes_topic: Some(format!("homeassistant/sensor/{}/raw", topic_root)),
            value_template: format!("{{{{ value_json.{} }}}}", id),

            expire_after: Some(60 * 60), // 1 hour
            icon: None,
            json_attributes_template: None,
            payload_on: true,
            payload_off: false,
        }
    }
    pub fn dont_expire(mut self) -> Self {
        self.expire_after = None;
        self
    }

    pub fn config_topic(&self) -> String {
        let mut topic_components: Vec<_> = self.state_topic.split("/").collect();
        topic_components.pop(); // Get rid of "/state"
        topic_components.pop(); // Get rid of ECU name
        topic_components.push(self.id); // Push sensor name
        topic_components.push("config");
        topic_components[1] = "binary_sensor";
        topic_components.join("/")
    }
}

async fn mqtt(mut rx: Receiver<(obd_data::Data, String)>) -> Result<()> {
    let mut mqttoptions = MqttOptions::new("ioniq2mqtt", "petschek.cc", 8883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let ca = include_bytes!("../certs/ca.crt");
    let client_cert = include_bytes!("../certs/client.crt");
    let client_key = include_bytes!("../certs/client.key");

    mqttoptions.set_transport(Transport::Tls(TlsConfiguration::Simple {
        ca: ca.to_vec(),
        alpn: None,
        client_auth: Some((client_cert.to_vec(), client_key.to_vec())),
    }));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_v) => {
                    // println!("Event = {v:?}");
                }
                Err(e) => {
                    println!("Error = {e:?}");
                }
            }
        }
    });

    let sensors = [
        HASSSensor::new("Charging Type", "charging", "enum", "ioniq/batterydata1"),
        HASSSensor::new("Aux Battery", "aux_battery_voltage", "voltage", "ioniq/batterydata1").with_unit("V"),
        HASSSensor::new("BMS SOC", "bms_soc", "battery", "ioniq/batterydata1").with_unit("%"),
        HASSSensor::new("Battery Current", "battery_current", "current", "ioniq/batterydata1").with_unit("A"),
        HASSSensor::new("Battery Voltage", "battery_voltage", "voltage", "ioniq/batterydata1").with_unit("V"),
        HASSSensor::new("Battery Power", "battery_power", "power", "ioniq/batterydata1").with_unit("kW"),
        HASSSensor::new("Fan Status", "fan_status", "", "ioniq/batterydata1"),
        HASSSensor::new("Fan Speed", "fan_speed", "frequency", "ioniq/batterydata1").with_unit("Hz"),
        HASSSensor::new("Cumulative Energy Charged", "cumulative_energy_charged", "energy", "ioniq/batterydata1").with_unit("kWh"),
        HASSSensor::new("Cumulative Energy Discharged", "cumulative_energy_discharged", "energy", "ioniq/batterydata1").with_unit("kWh"),
        HASSSensor::new("Cumulative Operating Time", "cumulative_operating_time", "duration", "ioniq/batterydata1").with_unit("s"),
        HASSSensor::new("DC Battery Inlet Temperature", "dc_battery_inlet_temp", "temperature", "ioniq/batterydata1").with_unit("C"),
        HASSSensor::new("DC Battery Max Temperature", "dc_battery_max_temp", "temperature", "ioniq/batterydata1").with_unit("C"),
        HASSSensor::new("DC Battery Min Temperature", "dc_battery_min_temp", "temperature", "ioniq/batterydata1").with_unit("C"),
        HASSSensor::new("DC Battery Max Cell Voltage", "dc_battery_cell_max_voltage", "voltage", "ioniq/batterydata1").with_unit("V"),
        HASSSensor::new("DC Battery Min Cell Voltage", "dc_battery_cell_min_voltage", "voltage", "ioniq/batterydata1").with_unit("V"),
        HASSSensor::new("Inverter Capacitor Voltage", "inverter_capacitor_voltage", "voltage", "ioniq/batterydata1").with_unit("V"),
        HASSSensor::new("Front Motor Speed", "front_drive_motor_speed", "frequency", "ioniq/batterydata1").with_unit("Hz"),
        HASSSensor::new("Rear Motor Speed", "rear_drive_motor_speed", "frequency", "ioniq/batterydata1").with_unit("Hz"),

        HASSSensor::new("SOC Display", "soc", "battery", "ioniq/batterydata5").with_unit("%").dont_expire(),
        HASSSensor::new("State of Health", "soh", "", "ioniq/batterydata5").with_unit("%"),
        HASSSensor::new("Available Charge Power", "available_charge_power", "power", "ioniq/batterydata5").with_unit("kW"),
        HASSSensor::new("Available Discharge Power", "available_discharge_power", "power", "ioniq/batterydata5").with_unit("kW"),
        HASSSensor::new("Battery Cell Voltage Deviation", "battery_cell_voltage_deviation", "voltage", "ioniq/batterydata5").with_unit("V"),
        HASSSensor::new("Battery Heater 1 Temperature", "battery_heater_1_temp", "temperature", "ioniq/batterydata5").with_unit("C"),
        HASSSensor::new("Battery Heater 2 Temperature", "battery_heater_2_temp", "temperature", "ioniq/batterydata5").with_unit("C"),
        HASSSensor::new("Remaining Energy", "remaining_energy", "energy_storage", "ioniq/batterydata5").with_unit("Wh"),

        HASSSensor::new("AC Charging Events", "ac_charging_events", "", "ioniq/batterydata11"),
        HASSSensor::new("DC Charging Events", "dc_charging_events", "", "ioniq/batterydata11"),
        HASSSensor::new("Cumulative AC Charging Energy", "cumulative_ac_charging_energy", "energy", "ioniq/batterydata11").with_unit("kWh"),
        HASSSensor::new("Cumulative DC Charging Energy", "cumulative_dc_charging_energy", "energy", "ioniq/batterydata11").with_unit("kWh"),

        HASSSensor::new("Front Left Tire Pressure", "front_left_psi", "pressure", "ioniq/tirepressures").with_unit("psi"),
        HASSSensor::new("Front Left Tire Temperature", "front_left_temp", "temperature", "ioniq/tirepressures").with_unit("C"),
        HASSSensor::new("Front Right Tire Pressure", "front_right_psi", "pressure", "ioniq/tirepressures").with_unit("psi"),
        HASSSensor::new("Front Right Tire Temperature", "front_right_temp", "temperature", "ioniq/tirepressures").with_unit("C"),
        HASSSensor::new("Rear Left Tire Pressure", "rear_left_psi", "pressure", "ioniq/tirepressures").with_unit("psi"),
        HASSSensor::new("Rear Left Tire Temperature", "rear_left_temp", "temperature", "ioniq/tirepressures").with_unit("C"),
        HASSSensor::new("Rear Right Tire Pressure", "rear_right_psi", "pressure", "ioniq/tirepressures").with_unit("psi"),
        HASSSensor::new("Rear Right Tire Temperature", "rear_right_temp", "temperature", "ioniq/tirepressures").with_unit("C"),

        HASSSensor::new("HVAC Indoor Temperature", "indoor_temp", "temperature", "ioniq/hvac").with_unit("C"),
        HASSSensor::new("HVAC Outdoor Temperature", "outdoor_temp", "temperature", "ioniq/hvac").with_unit("C"),
        HASSSensor::new("Vehicle Speed", "vehicle_speed", "speed", "ioniq/hvac").with_unit("km/h"),

        HASSSensor::new("Odometer", "odometer", "distance", "ioniq/dashboard").with_unit("mi").dont_expire(),

        HASSSensor::new("Cabin Pressure", "pressure", "atmospheric_pressure", "ioniq/cabinenvironment").with_unit("Pa"),
        HASSSensor::new("Cabin Temperature", "temperature", "temperature", "ioniq/cabinenvironment").with_unit("C"),
        HASSSensor::new("Cabin Humidity", "humidity", "humidity", "ioniq/cabinenvironment").with_unit("%"),

        HASSSensor::new("AC Maximum Current Limit", "ac_maximum_current_limit", "current", "ioniq/iccu01").with_unit("A"),
        HASSSensor::new("DC Maximum Current Limit", "dc_maximum_current_limit", "current", "ioniq/iccu01").with_unit("A"),
        HASSSensor::new("DC Target Voltage", "dc_target_voltage", "voltage", "ioniq/iccu01").with_unit("V"),
        HASSSensor::new("V2L AC Target Voltage", "v2l_ac_target_voltage", "voltage", "ioniq/iccu01").with_unit("V"),
        HASSSensor::new("V2L AC Current Limit", "v2l_ac_current_limit", "current", "ioniq/iccu01").with_unit("A"),
        HASSSensor::new("V2L DC Current Limit", "v2l_dc_current_limit", "current", "ioniq/iccu01").with_unit("A"),

        HASSSensor::new("OBC AC Current", "obc_ac_total_current", "current", "ioniq/iccu02").with_unit("A"),
        HASSSensor::new("OBC AC Voltage", "obc_ac_voltage_a", "voltage", "ioniq/iccu02").with_unit("V"),
        HASSSensor::new("OBC AC Voltage B", "obc_ac_voltage_b", "voltage", "ioniq/iccu02").with_unit("V"),
        HASSSensor::new("OBC AC Power", "obc_ac_power", "power", "ioniq/iccu02").with_unit("kW"),
        HASSSensor::new("OBC DC Current", "obc_dc_total_current", "current", "ioniq/iccu02").with_unit("A"),
        HASSSensor::new("OBC DC Voltage", "obc_dc_voltage", "voltage", "ioniq/iccu02").with_unit("V"),
        HASSSensor::new("OBC DC Target Current", "obc_dc_target_current", "current", "ioniq/iccu03").with_unit("A"),
        HASSSensor::new("OBC DC Target Voltage", "obc_dc_target_voltage", "voltage", "ioniq/iccu03").with_unit("V"),
        HASSSensor::new("OBC DC Power", "obc_dc_power", "power", "ioniq/iccu02").with_unit("kW"),
        HASSSensor::new("OBC Charging Loss", "obc_charging_loss", "power", "ioniq/iccu02").with_unit("kW"),
        HASSSensor::new("OBC Temperature 1", "obc_temp_a", "temperature", "ioniq/iccu02").with_unit("C"),
        HASSSensor::new("OBC Temperature 2", "obc_temp_b", "temperature", "ioniq/iccu02").with_unit("C"),

        HASSSensor::new("Aux Battery Current", "aux_battery_current", "current", "ioniq/iccu11").with_unit("A"),
        HASSSensor::new("Aux Battery SOC", "aux_battery_soc", "battery", "ioniq/iccu11").with_unit("%"),
        HASSSensor::new("Aux Battery Temperature", "aux_battery_temp", "temperature", "ioniq/iccu11").with_unit("C"),
        HASSSensor::new("Aux Battery Voltage", "aux_battery_voltage_iccu", "voltage", "ioniq/iccu11").with_unit("V"),
        HASSSensor::new("LDC Input Voltage", "ldc_input_voltage", "voltage", "ioniq/iccu11").with_unit("V"),
        HASSSensor::new("LDC Output Current", "ldc_output_current", "current", "ioniq/iccu11").with_unit("A"),
        HASSSensor::new("LDC Output Voltage", "ldc_output_voltage", "voltage", "ioniq/iccu11").with_unit("V"),
        HASSSensor::new("LDC Output Power", "ldc_output_power", "power", "ioniq/iccu11").with_unit("W"),
        HASSSensor::new("LDC Temperature", "ldc_temp", "temperature", "ioniq/iccu11").with_unit("C"),

        HASSSensor::new("AC Inlet Temperature", "ac_inlet_1_temperature", "temperature", "ioniq/vcms04").with_unit("C"),
        HASSSensor::new("DC Inlet Temperature 1", "dc_inlet_1_temperature", "temperature", "ioniq/vcms04").with_unit("C"),
        HASSSensor::new("DC Inlet Temperature 2", "dc_inlet_2_temperature", "temperature", "ioniq/vcms04").with_unit("C"),
        HASSSensor::new("V2L Discharge Current", "v2l_discharging_current", "current", "ioniq/vcms01").with_unit("A"),
        HASSSensor::new("Shifter Gear", "gear", "enum", "ioniq/shifter").dont_expire(),
    ];
    let binary_sensors = [
        HASSBinarySensor::new("Ignition On", "ignition_on", "power","ioniq/igpm03"),
        //HASSBinarySensor::new("Driver Seat Belt", "driver_seat_belt", "occupancy","ioniq/igpm03"),
        // HASSBinarySensor::new("Passenger Seat Belt", "passenger_seat_belt", "occupancy","ioniq/igpm03"),
        HASSBinarySensor::new("Hood", "hood_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Rear Right Door", "rear_right_door_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Rear Left Door", "rear_left_door_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Rear Right Unlocked", "rear_right_door_unlocked", "lock","ioniq/igpm03"),
        HASSBinarySensor::new("Rear Left Unlocked", "rear_left_door_unlocked", "lock","ioniq/igpm03"),
        HASSBinarySensor::new("Passenger Door", "passenger_door_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Driver Door", "driver_door_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Trunk", "trunk_open", "door","ioniq/igpm03"),
        HASSBinarySensor::new("Passenger Door Unlocked", "passenger_door_unlocked", "lock","ioniq/igpm04"),
        HASSBinarySensor::new("Driver Door Unlocked", "driver_door_unlocked", "lock","ioniq/igpm04"),
        HASSBinarySensor::new("Rear Left Seat Belt", "rear_left_seat_belt", "occupancy","ioniq/igpm04"),
        HASSBinarySensor::new("Rear Center Seat Belt", "rear_center_seat_belt", "occupancy","ioniq/igpm04"),
        HASSBinarySensor::new("Rear Right Seat Belt", "rear_right_seat_belt", "occupancy","ioniq/igpm04"),
    ];

    for sensor in sensors.iter() {
        client.publish(sensor.config_topic(), QoS::AtLeastOnce, true, serde_json::to_vec(sensor)?).await?;
    }
    for binary_sensor in binary_sensors.iter() {
        client.publish(binary_sensor.config_topic(), QoS::AtLeastOnce, true, serde_json::to_vec(binary_sensor)?).await?;
    }

    while let Some((forwarded_data, raw)) = rx.recv().await {
        let (topic_root, data) = match forwarded_data {
            obd_data::Data::Battery01(data) => ("homeassistant/sensor/ioniq/batterydata1", serde_json::to_vec(&data)?),
            obd_data::Data::Battery05(data) => ("homeassistant/sensor/ioniq/batterydata5", serde_json::to_vec(&data)?),
            obd_data::Data::Battery11(data) => ("homeassistant/sensor/ioniq/batterydata11", serde_json::to_vec(&data)?),
            obd_data::Data::TirePressures(data) => ("homeassistant/sensor/ioniq/tirepressures", serde_json::to_vec(&data)?),
            obd_data::Data::HVAC(data) => ("homeassistant/sensor/ioniq/hvac", serde_json::to_vec(&data)?),
            obd_data::Data::ICCU01(data) => ("homeassistant/sensor/ioniq/iccu01", serde_json::to_vec(&data)?),
            obd_data::Data::ICCU02(data) => ("homeassistant/sensor/ioniq/iccu02", serde_json::to_vec(&data)?),
            obd_data::Data::ICCU03(data) => ("homeassistant/sensor/ioniq/iccu03", serde_json::to_vec(&data)?),
            obd_data::Data::ICCU11(data) => ("homeassistant/sensor/ioniq/iccu11", serde_json::to_vec(&data)?),
            obd_data::Data::VCMS01(data) => ("homeassistant/sensor/ioniq/vcms01", serde_json::to_vec(&data)?),
            obd_data::Data::VCMS02(data) => ("homeassistant/sensor/ioniq/vcms02", serde_json::to_vec(&data)?),
            obd_data::Data::VCMS03(data) => ("homeassistant/sensor/ioniq/vcms03", serde_json::to_vec(&data)?),
            obd_data::Data::VCMS04(data) => ("homeassistant/sensor/ioniq/vcms04", serde_json::to_vec(&data)?),
            obd_data::Data::Dashboard(data) => ("homeassistant/sensor/ioniq/dashboard", serde_json::to_vec(&data)?),
            obd_data::Data::IGPM03(data) => ("homeassistant/sensor/ioniq/igpm03", serde_json::to_vec(&data)?),
            obd_data::Data::IGPM04(data) => ("homeassistant/sensor/ioniq/igpm04", serde_json::to_vec(&data)?),
            obd_data::Data::CabinEnvironment(data) => {
                client.publish("homeassistant/sensor/ioniq/cabinenvironment/state", QoS::AtLeastOnce, false, serde_json::to_vec(&data)?).await?;
                continue;
            },
            obd_data::Data::Shifter(data) => {
                client.publish("homeassistant/sensor/ioniq/shifter/state", QoS::AtLeastOnce, false, serde_json::to_vec(&data)?).await?;
                continue;
            },
        };
        client.publish(format!("{}/state", topic_root), QoS::AtLeastOnce, false, data).await?;
        client.publish(format!("{}/raw", topic_root), QoS::AtLeastOnce, false, format!("{{\"raw\": \"{}\"}}", raw).into_bytes()).await?;
    }
    Ok(())
}
