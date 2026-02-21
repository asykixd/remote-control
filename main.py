from remote_control.gui import ControlPanelApp
from remote_control.logging_setup import configure_logging


def main() -> None:
    log_path = configure_logging()
    app = ControlPanelApp(log_file_path=log_path)
    app.run()


if __name__ == "__main__":
    main()
