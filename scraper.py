import datetime

def collect_data():
    print("Collecting data...")
    with open("data.txt", "a") as f:
        f.write(f"Run at {datetime.datetime.now()}\n")

if __name__ == "__main__":
    collect_data()
