package me.jxl.kiosk_satellite

import android.content.Context
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodChannel
import kotlin.math.abs

/**
 * The ambient light sensor as a stream of lux values, so Home Assistant can
 * automate screen brightness from the light in the room.
 *
 * Damped at the source: a light sensor fires on every flicker and passing
 * shadow, and each event crossing the platform channel wakes Dart. An event
 * is forwarded when the value moved at least 5 lx and 10% since the last one
 * sent, and at most every 2 seconds; the first reading always passes so the
 * entity is never blank. Coarser rate limiting for the MQTT recorder lives on
 * the Dart side.
 *
 * TYPE_LIGHT needs no permission on any Android version. Devices without the
 * sensor (several Fire tablets) answer hasSensor=false and never get a
 * stream, so the entity is simply absent rather than dead.
 *
 * Some Android devices expose their ambient light sensor as a dynamic sensor
 * rather than as the default TYPE_LIGHT sensor. Prefer the normal default
 * sensor when available, then fall back to a dynamic TYPE_LIGHT sensor.
 */
class LightSensor(context: Context, messenger: BinaryMessenger) {
    private val sensorManager =
        context.getSystemService(Context.SENSOR_SERVICE) as SensorManager

    private val methods = MethodChannel(messenger, "kiosk_satellite/light_sensor")
    private val events = EventChannel(messenger, "kiosk_satellite/light_sensor_stream")

    private var sensor: Sensor? = findLightSensor()
    private var listener: SensorEventListener? = null
    private var activeSink: EventChannel.EventSink? = null

    private var lastSent = -1f
    private var lastSentAt = 0L
    private val handler = Handler(Looper.getMainLooper())

    /** Set by the first delivered sample; the register nudge stops on it. */
    @Volatile
    private var receivedAny = false

    /**
     * Prefer Android's default TYPE_LIGHT sensor. Some devices expose their
     * light sensor only through the dynamic sensor API, so fall back to the
     * first dynamic TYPE_LIGHT sensor when no default exists.
     */
    private fun findLightSensor(): Sensor? {
        return sensorManager.getDefaultSensor(Sensor.TYPE_LIGHT)
            ?: sensorManager
                .getDynamicSensorList(Sensor.TYPE_LIGHT)
                .firstOrNull()
    }

    /**
     * Re-evaluate the available light sensor and move an active stream to it
     * when necessary.
     */
    private fun refreshSensor() {
        val resolved = findLightSensor()

        if (resolved === sensor) return

        listener?.let { sensorManager.unregisterListener(it) }
        listener = null
        sensor = resolved

        if (resolved != null && activeSink != null) {
            registerListener(resolved, activeSink!!)
        }
    }

    /**
     * Register the lux listener for the selected sensor while preserving the
     * existing filtering and initial-sample retry behaviour.
     */
    private fun registerListener(
        targetSensor: Sensor,
        sink: EventChannel.EventSink,
    ) {
        listener?.let { sensorManager.unregisterListener(it) }

        lastSent = -1f
        lastSentAt = 0L
        receivedAny = false

        val l = object : SensorEventListener {
            override fun onSensorChanged(event: SensorEvent) {
                val lux = event.values.firstOrNull() ?: return
                receivedAny = true

                val now = SystemClock.elapsedRealtime()

                if (lastSent >= 0) {
                    val delta = abs(lux - lastSent)

                    if (
                        now - lastSentAt < 2000 ||
                        delta < 5f ||
                        delta < lastSent * 0.1f
                    ) {
                        return
                    }
                }

                lastSent = lux
                lastSentAt = now
                sink.success(lux.toDouble())
            }

            override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) {}
        }

        listener = l

        sensorManager.registerListener(
            l,
            targetSensor,
            SensorManager.SENSOR_DELAY_NORMAL,
        )

        // On-change sensors owe one sample at registration, but some drivers
        // lose it when registration races device boot. In a room where the
        // light then never changes, that silence can last indefinitely.
        // Re-registering coerces the initial sample; give the driver a few
        // chances, then leave it to the first genuine change.
        fun nudge(remaining: Int) {
            handler.postDelayed({
                if (
                    receivedAny ||
                    listener !== l ||
                    sensor !== targetSensor
                ) {
                    return@postDelayed
                }

                sensorManager.unregisterListener(l)

                sensorManager.registerListener(
                    l,
                    targetSensor,
                    SensorManager.SENSOR_DELAY_NORMAL,
                )

                if (remaining > 1) {
                    nudge(remaining - 1)
                }
            }, 4_000)
        }

        nudge(3)
    }

    private val dynamicSensorCallback =
        object : SensorManager.DynamicSensorCallback() {
            override fun onDynamicSensorConnected(connectedSensor: Sensor) {
                if (connectedSensor.type != Sensor.TYPE_LIGHT) return

                handler.post {
                    refreshSensor()
                }
            }

            override fun onDynamicSensorDisconnected(disconnectedSensor: Sensor) {
                if (disconnectedSensor.type != Sensor.TYPE_LIGHT) return

                handler.post {
                    if (sensor === disconnectedSensor) {
                        listener?.let { sensorManager.unregisterListener(it) }
                        listener = null
                        sensor = null
                    }

                    refreshSensor()
                }
            }
        }

    init {
        sensorManager.registerDynamicSensorCallback(
            dynamicSensorCallback,
            handler,
        )

        methods.setMethodCallHandler { call, result ->
            when (call.method) {
                "hasSensor" -> {
                    sensor = findLightSensor()
                    result.success(sensor != null)
                }

                else -> result.notImplemented()
            }
        }

        events.setStreamHandler(
            object : EventChannel.StreamHandler {
                override fun onListen(
                    args: Any?,
                    sink: EventChannel.EventSink,
                ) {
                    activeSink = sink

                    sensor = findLightSensor()

                    val s = sensor

                    if (s == null) {
                        /*
                         * Keep the stream alive rather than ending it. A
                         * dynamic TYPE_LIGHT sensor may be registered after
                         * Kiosk Satellite starts; dynamicSensorCallback will
                         * attach to it when it appears.
                         */
                        return
                    }

                    registerListener(s, sink)
                }

                override fun onCancel(args: Any?) {
                    activeSink = null

                    listener?.let {
                        sensorManager.unregisterListener(it)
                    }

                    listener = null
                    lastSent = -1f
                    lastSentAt = 0L
                    receivedAny = false
                }
            },
        )
    }

    fun dispose() {
        activeSink = null

        listener?.let {
            sensorManager.unregisterListener(it)
        }

        listener = null

        sensorManager.unregisterDynamicSensorCallback(
            dynamicSensorCallback,
        )

        methods.setMethodCallHandler(null)
        events.setStreamHandler(null)
    }
}
