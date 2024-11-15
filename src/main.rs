mod obd_data;
mod hass;

use std::time::{Duration, Instant};
use std::sync::Arc;
use capnp::serialize;
use tmq::Context;
use anyhow::Result;
use tokio::sync::broadcast::{self, Sender, Receiver};
use tokio::sync::Mutex;
use tokio::time;
use futures::StreamExt;
use rumqttc::{AsyncClient, MqttOptions, QoS, TlsConfiguration, Transport};
use serde::Serialize;
use crate::obd_data::{ChargingType, Gear, Process};
use crate::hass::{ HASSSensor, HASSBinarySensor };

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

#[derive(Serialize, Debug, Default)]
struct Telemetry {
    #[serde(skip_serializing_if = "Option::is_none")]
    utc: Option<i64>, // Seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    soc: Option<f32>, // From display
    #[serde(skip_serializing_if = "Option::is_none")]
    power: Option<f32>, // kW
    #[serde(skip_serializing_if = "Option::is_none")]
    speed: Option<u8>, // kmh
    #[serde(skip_serializing_if = "Option::is_none")]
    lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lon: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_charging: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_dcfc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_parked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    soe: Option<f32>, // kWh, usable energy of battery
    #[serde(skip_serializing_if = "Option::is_none")]
    soh: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    heading: Option<f32>, // degrees
    #[serde(skip_serializing_if = "Option::is_none")]
    elevation: Option<f64>, // meters
    #[serde(skip_serializing_if = "Option::is_none")]
    ext_temp: Option<f32>, // C
    #[serde(skip_serializing_if = "Option::is_none")]
    batt_temp: Option<f32>, // C
    voltage: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    odometer: Option<u32>, // km
    // hvac_power
    // hvac_setpoint
    #[serde(skip_serializing_if = "Option::is_none")]
    cabin_temp: Option<f32>, // C
    #[serde(skip_serializing_if = "Option::is_none")]
    tire_pressure_fl: Option<f32>, // kPa
    #[serde(skip_serializing_if = "Option::is_none")]
    tire_pressure_fr: Option<f32>, // kPa
    #[serde(skip_serializing_if = "Option::is_none")]
    tire_pressure_rl: Option<f32>, // kPa
    #[serde(skip_serializing_if = "Option::is_none")]
    tire_pressure_rr: Option<f32>, // kPa
}

#[tokio::main]
async fn main() {
    let (tx, rx) = broadcast::channel::<(obd_data::Data, String)>(16);
    let telemetry_update_rx = tx.subscribe();

    let telemetry = Arc::new(Mutex::new(Telemetry::default()));

    let mut tasks = vec![];

    tasks.push(tokio::spawn(mqtt(rx)));
    tasks.push(tokio::spawn(update_can(tx)));
    tasks.push(tokio::spawn(update_telemetry_with_can(telemetry_update_rx, telemetry.clone())));
    tasks.push(tokio::spawn(update_location(telemetry.clone())));
    tasks.push(tokio::spawn(abrp(telemetry.clone())));

    let errored = futures::future::select_all(tasks).await;
    println!("A task exited!!");
    dbg!(&errored);
    if let Err(err) = errored.0.unwrap() {
        eprintln!("Task errored: {:?}", err);
    }
}

async fn update_can(tx: Sender<(obd_data::Data, String)>) -> Result<()> {
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
                            // For debug logging only
                            #[cfg(debug_assertions)]
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
                                tx.send((processed_data, raw))?;
                            }
                        }
                        else if can_event.get_src() == 1 && can_event.get_address() == 0x130 {
                            if last_shifter_message.elapsed().as_millis() > 1000 {
                                // Gear shifter message
                                let processed_data = obd_data::Shifter::process(data);
                                if let Some(processed_data) = processed_data {
                                    tx.send((processed_data, String::new()))?;
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

async fn update_telemetry_with_can(mut rx: Receiver<(obd_data::Data, String)>, telemetry: Arc<Mutex<Telemetry>>) -> Result<()> {
    while let Ok((forwarded_data, _raw)) = rx.recv().await {
        let mut telemetry = telemetry.lock().await;
        match forwarded_data {
            obd_data::Data::Battery01(data) => {
                telemetry.power = Some(data.battery_power);
                telemetry.is_charging = Some(data.charging != ChargingType::NotCharging);
                telemetry.is_dcfc = Some(data.charging == ChargingType::DC);
                telemetry.batt_temp = Some((data.dc_battery_max_temp as f32 + data.dc_battery_min_temp as f32) / 2.0);
                telemetry.voltage = Some(data.battery_voltage);
                telemetry.current = Some(data.battery_current);
            },
            obd_data::Data::Battery05(data) => {
                telemetry.soc = Some(data.soc);
                telemetry.soh = Some(data.soh);
                telemetry.soe = Some(data.remaining_energy / 1000.0);
            },
            obd_data::Data::TirePressures(data) => {
                telemetry.tire_pressure_fl = Some(data.front_left_psi * 6.89476);
                telemetry.tire_pressure_fr = Some(data.front_right_psi * 6.89476);
                telemetry.tire_pressure_rl = Some(data.rear_left_psi * 6.89476);
                telemetry.tire_pressure_rr = Some(data.rear_right_psi * 6.89476);
            },
            obd_data::Data::HVAC(data) => {
                telemetry.speed = Some(data.vehicle_speed);
                telemetry.ext_temp = Some(data.outdoor_temp);
                telemetry.cabin_temp = Some(data.indoor_temp);
            },
            obd_data::Data::Dashboard(data) => {
                telemetry.odometer = Some((data.odometer as f32 * 1.609344) as u32);
            },
            obd_data::Data::Shifter(data) => {
                telemetry.is_parked = Some(data.gear == Gear::Park);
            },
            _ => {},
        };
    }
    Ok(())
}

async fn update_location(telemetry: Arc<Mutex<Telemetry>>) -> Result<()> {
    let mut socket = tmq::subscribe(&Context::new())
        .connect("tcp://127.0.0.1:30590")? // Port for "gpsLocation"
        .subscribe(&[])?;

    #[derive(Debug)]
    struct Location {
        latitude: f64,
        longitude: f64,
        altitude: f64,
        speed: f32,
        bearing: f32,
        unix_timestamp_seconds: i64,
        vertical_accuracy: f32,
        bearing_accuracy: f32,
        speed_accuracy: f32,
        has_fix: bool,
    }
    let mut current_location: Option<Location> = None;

    while let Some(messages) = socket.next().await {
        for message in messages? {
            let message_reader = serialize::read_message(
                &*message,
                capnp::message::ReaderOptions::new(),
            )?;
            let event = message_reader.get_root::<log_capnp::event::Reader>()?;
            match event.which()? {
                log_capnp::event::GpsLocation(Ok(location_data)) => {
                    let location = Location {
                        latitude: location_data.get_latitude(),
                        longitude: location_data.get_longitude(),
                        altitude: location_data.get_altitude(),
                        speed: location_data.get_speed(),
                        bearing: location_data.get_bearing_deg(),
                        unix_timestamp_seconds: location_data.get_unix_timestamp_millis() / 1000,
                        vertical_accuracy: location_data.get_vertical_accuracy(),
                        bearing_accuracy: location_data.get_bearing_accuracy_deg(),
                        speed_accuracy: location_data.get_speed_accuracy(),
                        has_fix: location_data.get_has_fix(),
                    };

                    if location.has_fix && location.unix_timestamp_seconds > 0 {
                        current_location.replace(location);
                    }
                },
                _ => {},
            }
        }
        if let Some(location) = current_location.take() {
            let mut telemetry = telemetry.lock().await;
            telemetry.utc = Some(location.unix_timestamp_seconds);
            telemetry.lat = Some(location.latitude);
            telemetry.lon = Some(location.longitude);
            telemetry.heading = Some(location.bearing);
            telemetry.elevation = Some(location.altitude);
        }
    }
    Ok(())
}

async fn abrp(telemetry: Arc<Mutex<Telemetry>>) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut last_sent_time = i64::default();

    loop {
        interval.tick().await;
        let telemetry = {
            let telemetry = telemetry.lock().await;
            if telemetry.utc.is_none() || telemetry.utc.unwrap() == last_sent_time {
                continue;
            }
            last_sent_time = telemetry.utc.unwrap();
            serde_json::to_string(&*telemetry)?
        };

        let client = reqwest::Client::new();
        let response = client.post("https://api.iternio.com/1/tlm/send")
            .form(&[
                ("api_key", include_str!("../certs/abrp.apikey").trim()),
                ("token", include_str!("../certs/abrp.usertoken").trim()),
                ("tlm", &telemetry),
            ])
            .timeout(Duration::from_secs(5))
            .send().await;
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                eprintln!("ABRP Error = {err:?}");
                continue;
            }
        };
        let response_body = response.text().await?;
        if response_body != "{\"status\": \"ok\"}" {
            dbg!(&telemetry);
            dbg!(&response_body);
        }
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
                    eprintln!("MQTT Error = {e:?}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    });

    let sensors = [
        HASSSensor::new("Charging Type", "charging", "enum", "ioniq/batterydata1"),
        HASSSensor::new("Aux Battery", "aux_battery_voltage", "voltage", "ioniq/batterydata1").with_unit("V").measurement(),
        HASSSensor::new("BMS SOC", "bms_soc", "battery", "ioniq/batterydata1").with_unit("%").measurement(),
        HASSSensor::new("Battery Current", "battery_current", "current", "ioniq/batterydata1").with_unit("A").measurement(),
        HASSSensor::new("Battery Voltage", "battery_voltage", "voltage", "ioniq/batterydata1").with_unit("V").measurement(),
        HASSSensor::new("Battery Power", "battery_power", "power", "ioniq/batterydata1").with_unit("kW").measurement(),
        HASSSensor::new("Fan Status", "fan_status", "", "ioniq/batterydata1").measurement(),
        HASSSensor::new("Fan Speed", "fan_speed", "frequency", "ioniq/batterydata1").with_unit("Hz").measurement(),
        HASSSensor::new("Cumulative Energy Charged", "cumulative_energy_charged", "energy", "ioniq/batterydata1").with_unit("kWh").total_increasing(),
        HASSSensor::new("Cumulative Energy Discharged", "cumulative_energy_discharged", "energy", "ioniq/batterydata1").with_unit("kWh").total_increasing(),
        HASSSensor::new("Cumulative Operating Time", "cumulative_operating_time", "duration", "ioniq/batterydata1").with_unit("s").total_increasing(),
        HASSSensor::new("DC Battery Inlet Temperature", "dc_battery_inlet_temp", "temperature", "ioniq/batterydata1").with_unit("C").measurement(),
        HASSSensor::new("DC Battery Max Temperature", "dc_battery_max_temp", "temperature", "ioniq/batterydata1").with_unit("C").measurement(),
        HASSSensor::new("DC Battery Min Temperature", "dc_battery_min_temp", "temperature", "ioniq/batterydata1").with_unit("C").measurement(),
        HASSSensor::new("DC Battery Max Cell Voltage", "dc_battery_cell_max_voltage", "voltage", "ioniq/batterydata1").with_unit("V").measurement(),
        HASSSensor::new("DC Battery Min Cell Voltage", "dc_battery_cell_min_voltage", "voltage", "ioniq/batterydata1").with_unit("V").measurement(),
        HASSSensor::new("Inverter Capacitor Voltage", "inverter_capacitor_voltage", "voltage", "ioniq/batterydata1").with_unit("V").measurement(),
        HASSSensor::new("Front Motor Speed", "front_drive_motor_speed", "frequency", "ioniq/batterydata1").with_unit("Hz").measurement(),
        HASSSensor::new("Rear Motor Speed", "rear_drive_motor_speed", "frequency", "ioniq/batterydata1").with_unit("Hz").measurement(),

        HASSSensor::new("SOC Display", "soc", "battery", "ioniq/batterydata5").with_unit("%").dont_expire().measurement(),
        HASSSensor::new("State of Health", "soh", "", "ioniq/batterydata5").with_unit("%").measurement(),
        HASSSensor::new("Available Charge Power", "available_charge_power", "power", "ioniq/batterydata5").with_unit("kW").measurement(),
        HASSSensor::new("Available Discharge Power", "available_discharge_power", "power", "ioniq/batterydata5").with_unit("kW").measurement(),
        HASSSensor::new("Battery Cell Voltage Deviation", "battery_cell_voltage_deviation", "voltage", "ioniq/batterydata5").with_unit("V").measurement(),
        HASSSensor::new("Battery Heater 1 Temperature", "battery_heater_1_temp", "temperature", "ioniq/batterydata5").with_unit("C").measurement(),
        HASSSensor::new("Battery Heater 2 Temperature", "battery_heater_2_temp", "temperature", "ioniq/batterydata5").with_unit("C").measurement(),
        HASSSensor::new("Remaining Energy", "remaining_energy", "energy_storage", "ioniq/batterydata5").with_unit("Wh").measurement(),

        HASSSensor::new("AC Charging Events", "ac_charging_events", "", "ioniq/batterydata11").total_increasing(),
        HASSSensor::new("DC Charging Events", "dc_charging_events", "", "ioniq/batterydata11").total_increasing(),
        HASSSensor::new("Cumulative AC Charging Energy", "cumulative_ac_charging_energy", "energy", "ioniq/batterydata11").with_unit("kWh").total_increasing(),
        HASSSensor::new("Cumulative DC Charging Energy", "cumulative_dc_charging_energy", "energy", "ioniq/batterydata11").with_unit("kWh").total_increasing(),

        HASSSensor::new("Front Left Tire Pressure", "front_left_psi", "pressure", "ioniq/tirepressures").with_unit("psi").measurement(),
        HASSSensor::new("Front Left Tire Temperature", "front_left_temp", "temperature", "ioniq/tirepressures").with_unit("C").measurement(),
        HASSSensor::new("Front Right Tire Pressure", "front_right_psi", "pressure", "ioniq/tirepressures").with_unit("psi").measurement(),
        HASSSensor::new("Front Right Tire Temperature", "front_right_temp", "temperature", "ioniq/tirepressures").with_unit("C").measurement(),
        HASSSensor::new("Rear Left Tire Pressure", "rear_left_psi", "pressure", "ioniq/tirepressures").with_unit("psi").measurement(),
        HASSSensor::new("Rear Left Tire Temperature", "rear_left_temp", "temperature", "ioniq/tirepressures").with_unit("C").measurement(),
        HASSSensor::new("Rear Right Tire Pressure", "rear_right_psi", "pressure", "ioniq/tirepressures").with_unit("psi").measurement(),
        HASSSensor::new("Rear Right Tire Temperature", "rear_right_temp", "temperature", "ioniq/tirepressures").with_unit("C").measurement(),

        HASSSensor::new("HVAC Indoor Temperature", "indoor_temp", "temperature", "ioniq/hvac").with_unit("C").measurement(),
        HASSSensor::new("HVAC Outdoor Temperature", "outdoor_temp", "temperature", "ioniq/hvac").with_unit("C").measurement(),
        HASSSensor::new("Vehicle Speed", "vehicle_speed", "speed", "ioniq/hvac").with_unit("km/h").measurement(),

        HASSSensor::new("Odometer", "odometer", "distance", "ioniq/dashboard").with_unit("mi").dont_expire().total_increasing(),

        HASSSensor::new("Cabin Pressure", "pressure", "atmospheric_pressure", "ioniq/cabinenvironment").with_unit("Pa").measurement(),
        HASSSensor::new("Cabin Temperature", "temperature", "temperature", "ioniq/cabinenvironment").with_unit("C").measurement(),
        HASSSensor::new("Cabin Humidity", "humidity", "humidity", "ioniq/cabinenvironment").with_unit("%").measurement(),

        HASSSensor::new("AC Maximum Current Limit", "ac_maximum_current_limit", "current", "ioniq/iccu01").with_unit("A").measurement(),
        HASSSensor::new("DC Maximum Current Limit", "dc_maximum_current_limit", "current", "ioniq/iccu01").with_unit("A").measurement(),
        HASSSensor::new("DC Target Voltage", "dc_target_voltage", "voltage", "ioniq/iccu01").with_unit("V").measurement(),
        HASSSensor::new("V2L AC Target Voltage", "v2l_ac_target_voltage", "voltage", "ioniq/iccu01").with_unit("V").measurement(),
        HASSSensor::new("V2L AC Current Limit", "v2l_ac_current_limit", "current", "ioniq/iccu01").with_unit("A").measurement(),
        HASSSensor::new("V2L DC Current Limit", "v2l_dc_current_limit", "current", "ioniq/iccu01").with_unit("A").measurement(),

        HASSSensor::new("OBC AC Current", "obc_ac_total_current", "current", "ioniq/iccu02").with_unit("A").measurement(),
        HASSSensor::new("OBC AC Voltage", "obc_ac_voltage_a", "voltage", "ioniq/iccu02").with_unit("V").measurement(),
        HASSSensor::new("OBC AC Voltage B", "obc_ac_voltage_b", "voltage", "ioniq/iccu02").with_unit("V").measurement(),
        HASSSensor::new("OBC AC Power", "obc_ac_power", "power", "ioniq/iccu02").with_unit("kW").measurement(),
        HASSSensor::new("OBC DC Current", "obc_dc_total_current", "current", "ioniq/iccu02").with_unit("A").measurement(),
        HASSSensor::new("OBC DC Voltage", "obc_dc_voltage", "voltage", "ioniq/iccu02").with_unit("V").measurement(),
        HASSSensor::new("OBC DC Target Current", "obc_dc_target_current", "current", "ioniq/iccu03").with_unit("A").measurement(),
        HASSSensor::new("OBC DC Target Voltage", "obc_dc_target_voltage", "voltage", "ioniq/iccu03").with_unit("V").measurement(),
        HASSSensor::new("OBC DC Power", "obc_dc_power", "power", "ioniq/iccu02").with_unit("kW").measurement(),
        HASSSensor::new("OBC Charging Loss", "obc_charging_loss", "power", "ioniq/iccu02").with_unit("kW").measurement(),
        HASSSensor::new("OBC Temperature 1", "obc_temp_a", "temperature", "ioniq/iccu02").with_unit("C").measurement(),
        HASSSensor::new("OBC Temperature 2", "obc_temp_b", "temperature", "ioniq/iccu02").with_unit("C").measurement(),

        HASSSensor::new("Aux Battery Current", "aux_battery_current", "current", "ioniq/iccu11").with_unit("A").measurement(),
        HASSSensor::new("Aux Battery SOC", "aux_battery_soc", "battery", "ioniq/iccu11").with_unit("%").measurement(),
        HASSSensor::new("Aux Battery Temperature", "aux_battery_temp", "temperature", "ioniq/iccu11").with_unit("C").measurement(),
        HASSSensor::new("Aux Battery Voltage", "aux_battery_voltage_iccu", "voltage", "ioniq/iccu11").with_unit("V").measurement(),
        HASSSensor::new("LDC Input Voltage", "ldc_input_voltage", "voltage", "ioniq/iccu11").with_unit("V").measurement(),
        HASSSensor::new("LDC Output Current", "ldc_output_current", "current", "ioniq/iccu11").with_unit("A").measurement(),
        HASSSensor::new("LDC Output Voltage", "ldc_output_voltage", "voltage", "ioniq/iccu11").with_unit("V").measurement(),
        HASSSensor::new("LDC Output Power", "ldc_output_power", "power", "ioniq/iccu11").with_unit("W").measurement(),
        HASSSensor::new("LDC Temperature", "ldc_temp", "temperature", "ioniq/iccu11").with_unit("C").measurement(),

        HASSSensor::new("AC Inlet Temperature", "ac_inlet_1_temperature", "temperature", "ioniq/vcms04").with_unit("C").measurement(),
        HASSSensor::new("DC Inlet Temperature 1", "dc_inlet_1_temperature", "temperature", "ioniq/vcms04").with_unit("C").measurement(),
        HASSSensor::new("DC Inlet Temperature 2", "dc_inlet_2_temperature", "temperature", "ioniq/vcms04").with_unit("C").measurement(),
        HASSSensor::new("V2L Discharge Current", "v2l_discharging_current", "current", "ioniq/vcms01").with_unit("A").measurement(),
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

    loop {
        match rx.recv().await {
            Ok((forwarded_data, raw)) => {
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
            },
            Err(broadcast::error::RecvError::Lagged(lag_count)) => {
                println!("MQTT send lagged by {} message(s)", lag_count);
            },
            Err(err) => anyhow::bail!(err),
        }
    }
}
