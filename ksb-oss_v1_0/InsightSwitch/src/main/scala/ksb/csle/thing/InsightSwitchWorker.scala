package ksb.csle.thing

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import com.palominolabs.wemo.InsightSwitch;
import com.palominolabs.wemo.InsightSwitchOperationException;
import com.palominolabs.wemo.PowerUsage;
import com.palominolabs.wemo.PowerUsage.State;

class InsightSwitchWorker(private var insightSwitch: InsightSwitch,
                          private var proxy: InsightSwitchProxy)
    extends Runnable {

  var sensingPeriod: Long = this.proxy.config.getSensingPeriod()
  var sensingTimer: Timer = new Timer()
  var bSwitchON: Boolean = false

  def setOn(): Unit = {
    this.insightSwitch.switchOn()
  }

  def setOff(): Unit = {
    this.insightSwitch.switchOff()
  }

  def run(): Unit = {
    try this.proxy.addDevice(getId, this)
    catch {
      case e: Exception => LOG.error(e.getMessage, e)
    }
  }

  def getId(): String = this.insightSwitch.getFriendlyName

  def unregister(): Unit = {
    this.sensingTimer.cancel()
  }

  def close(): Unit = {
    this.sensingTimer.cancel()
    this.proxy.removeDevice(getId)
  }

  private class SwitchMon(private var insightSwitch: InsightSwitch)
      extends TimerTask {
    override def run(): Unit = {}
  }

  private def checkSwitchState(switchOn: State): Boolean = switchOn match {
    case State.OFF =>
      this.bSwitchON = false
      false
    case _ =>
      this.bSwitchON = true
      true
  }

  override def toString(): String = this.insightSwitch.getFriendlyName

}
