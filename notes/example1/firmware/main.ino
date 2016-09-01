int count = 0;

void setup() {

}

void loop() {
    if ((count % 5) == 0) {
        Particle.publish("device_count_alert", String(count));
    }

    count++;
    delay(1500);
}