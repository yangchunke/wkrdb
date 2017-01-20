package net.yck.wrkdb.server;

import java.util.concurrent.atomic.AtomicBoolean;

import net.yck.wkrdb.common.shared.AppBase.Component;

abstract class ServerComponent extends Component {

  private AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  ServerComponent(App app) {
    super(app);
  }

  protected abstract void doShutdown();

  void shutdown() {
    if (shutdownInProgress.compareAndSet(false, true)) {
      doShutdown();
    }
  }

}
