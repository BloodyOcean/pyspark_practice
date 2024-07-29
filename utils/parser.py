import configparser


class ConfigReader:
    def __init__(self, config_file: str):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get(self, section, option, fallback=None):
        """Retrieve a value from the configuration file."""
        return self.config.get(section, option, fallback=fallback)

    def get_int(self, section, option, fallback=0):
        """Retrieve an integer value from the configuration file."""
        return self.config.getint(section, option, fallback=fallback)

    def get_float(self, section, option, fallback=0.0):
        """Retrieve a float value from the configuration file."""
        return self.config.getfloat(section, option, fallback=fallback)

    def get_boolean(self, section, option, fallback=False):
        """Retrieve a boolean value from the configuration file."""
        return self.config.getboolean(section, option, fallback=fallback)

    def get_section(self, section):
        """Retrieve all key-value pairs in a section as a dictionary."""
        if section in self.config:
            return dict(self.config.items(section))
        return {}


# Example usage
if __name__ == "__main__":
    config_reader = ConfigReader("config.ini")
    database_host = config_reader.get("database", "host", fallback="localhost")
    database_port = config_reader.get_int("database", "port", fallback=5432)
    use_ssl = config_reader.get_boolean("database", "use_ssl", fallback=True)

    print(f"Host: {database_host}, Port: {database_port}, Use SSL: {use_ssl}")
