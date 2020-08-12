import file_and_table_name_sender
import plot_display
import table_loader_receiver
import subprocess


def main():
    process1 = subprocess.Popen(["python", "plot_display.py"])
    process2 = subprocess.Popen(["python", "table_loader_receiver.py"])
    process3 = subprocess.Popen(["python", "file_and_table_name_sender.py"])

    process1.wait()
    process2.wait()
    process3.wait()


if __name__ == '__main__':
    main()
