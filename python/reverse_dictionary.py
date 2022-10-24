my_dict = {"color": "red", "width": 17, "height": 19}
value_to_find = "red"

# Brute force solution (fastest) -- single key
for key, value in my_dict.items():
    if value == value_to_find:
        print(f'{key}: {value}')
        break

# Brute force solution -- multiple keys
for key, value in my_dict.items():
    if value == value_to_find:
        print(f'{key}: {value}')

# Generator expression -- single key
key = next(key for key, value in my_dict.items() if value == value_to_find)
print(f'{key}: {value_to_find}')

# Generator expression -- multiple keys
exp = (key for key, value in my_dict.items() if value == value_to_find)
for key in exp:
    print(f'{key}: {value}')

# Inverse dictionary solution -- single key
my_inverted_dict = {value: key for key, value in my_dict.items()}
print(f'{my_inverted_dict[value_to_find]}: {value_to_find}')

# Inverse dictionary solution (slowest) -- multiple keys
my_inverted_dict = dict()
for key, value in my_dict.items():
    my_inverted_dict.setdefault(value, list()).append(key)

print(f'{my_inverted_dict[value_to_find]}: {value_to_find}')
