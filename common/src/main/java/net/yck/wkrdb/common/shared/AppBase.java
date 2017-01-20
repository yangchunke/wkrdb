package net.yck.wkrdb.common.shared;

public abstract class AppBase {

  public final Configurator config;

  public AppBase(String args[]) {
    config = configuratorBuilder(args).build();
  }

  protected Configurator.Builder configuratorBuilder(String args[]) {
    return Configurator.builder().addOption(Configurator.OPT_CONFIGURATION).addOption(Configurator.OPT_PROPERTY)
        .args(args);
  }

  public abstract static class Component implements IConfigurable {

    public final AppBase app;

    protected Component(AppBase app) {
      this.app = app;
    }

    @SuppressWarnings("unchecked")
    public final <T extends Component> T initialize() throws Exception {
      doInitialize();
      return (T) this;
    }

    protected abstract void doInitialize() throws Exception;
  }

}
