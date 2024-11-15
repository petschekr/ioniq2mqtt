use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct HASSDevice<'a> {
    identifiers: &'a str,
    manufacturer: &'a str,
    model: &'a str,
    name: &'a str,
    serial_number: &'a str,
    sw_version: &'a str,
}
pub const IONIQ_5: HASSDevice = HASSDevice {
    manufacturer: "Hyundai",
    model: "Ioniq 5",
    name: "Ioniq 5",
    serial_number: "KM8KRDDF1RU297513",
    identifiers: "KM8KRDDF1RU297513",
    sw_version: "0.1.0",
};
#[derive(Serialize, Debug)]
pub struct HASSSensor<'a> {
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
    pub fn measurement(mut self) -> Self {
        self.state_class = Some("MEASUREMENT".into());
        self
    }
    pub fn total(mut self) -> Self {
        self.state_class = Some("TOTAL".into());
        self
    }
    pub fn total_increasing(mut self) -> Self {
        self.state_class = Some("TOTAL_INCREASE".into());
        self
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
pub struct HASSBinarySensor<'a> {
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