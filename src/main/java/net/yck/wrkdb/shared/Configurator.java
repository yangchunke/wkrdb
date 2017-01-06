package net.yck.wrkdb.shared;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Configurator {

    private final static Logger LOG = LogManager.getLogger(Configurator.class);

    public static Option OPT_CONFIGURATION = Option.builder("c").longOpt("configuration").argName("configuration file").hasArg().build();

    public static Option OPT_PROPERTY = Option.builder("D").longOpt("property").argName("property=value").hasArgs().valueSeparator('=').build();

    private final Options options;
    private CommandLine line;
    private Properties properties;
    private Configuration configuration;

    public Configurator(Options options) {
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key, T def) {
        if (properties != null && properties.containsKey(key)) {
            return (T) PropertyConverter.to(def.getClass(), properties.get(key));
        }

        if (line.hasOption(key)) {
            return (T) PropertyConverter.to(def.getClass(), line.getOptionValue(key));
        }

        if (configuration != null && configuration.containsKey(key)) {
            return (T) configuration.get(def.getClass(), key);
        }

        return def;
    }

    private Configurator parseArgs(String[] args) {

        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            if (line.hasOption(OPT_PROPERTY.getOpt())) {
                properties = line.getOptionProperties(OPT_PROPERTY.getOpt());
            }

            if (line.hasOption(OPT_CONFIGURATION.getOpt())) {
                Configurations configs = new Configurations();
                configuration = configs.properties(line.getOptionValue(OPT_CONFIGURATION.getOpt()));
            }
        }
        catch (ParseException exp) {
            LOG.error(() -> "Parsing failed.", exp);
        }
        catch (ConfigurationException e) {
            LOG.error(() -> "Loading configuration file failed.", e);
        }

        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String[] args;
        private final Options options = new Options();

        public Configurator build() {
            return new Configurator(options).parseArgs(args);
        }

        public Builder args(String[] args) {
            this.args = args;
            return this;
        }

        public Builder addSwitch(String opt, String longOpt, String desc) {
            options.addOption(Option.builder(opt).longOpt(longOpt).desc(desc).build());
            return this;
        }

        public <T> Builder addOption(String opt, String longOpt, String argName, String desc) {
            options.addOption(Option.builder(opt).longOpt(longOpt).hasArg().argName(argName).desc(desc).build());
            return this;
        }

        public Builder addOption(Option opt) {
            options.addOption(opt);
            return this;
        }
    }

}
