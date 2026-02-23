from app.harvester import main


if __name__ == "__main__":
    code, message = main()
    if message:
        print(message)
    raise SystemExit(code)
