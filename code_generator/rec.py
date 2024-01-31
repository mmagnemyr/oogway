def print_ascii_art(file_path, font_size):
    # Open the ASCII art file and read its content
    with open(file_path, 'r') as file:
        ascii_art = file.read()

    # Split the ASCII art into lines
    lines = ascii_art.splitlines()

    # Print each line with the specified font size
    for line in lines:
        print(line)

if __name__ == "__main__":
    # Specify the path to the ASCII art file
    ascii_art_file = 'oogway_t.ink'

    # Specify the desired font size (number of characters per line)
    desired_font_size = 40

    # Call the function to print the ASCII art with the desired font size
    print_ascii_art(ascii_art_file, desired_font_size)
