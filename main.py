from src.gui import ControlPanelApp
from src.logging_setup import configure_logging


def main() -> None:
    log_path = configure_logging()
    app = ControlPanelApp(log_file_path=log_path)
    app.run()


if __name__ == "__main__":
    main()
