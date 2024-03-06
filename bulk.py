from feeder import RecruitsUploader

u = RecruitsUploader()

with open("list.txt", "r") as input_file:
    waves = [row.strip() for row in input_file]

for wave in waves:
    u.config["project_name"] = wave
    u.run()
