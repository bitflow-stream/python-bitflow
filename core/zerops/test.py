from functools import reduce

import torch







exit()

value = {"test": "asd", "test1": "rets"}
size = reduce(lambda x, y: x + y, map(lambda x: len(x), value.values()))

print(size)

exit()

def get_n_best_prediction(lst, n, skip_indices=None):
    if n > len(lst):
        raise ValueError("n cannot be greater than the number of predicted classes. Maximal value for n can be {} "
                         "but n is actually {}".format(len(lst), n))
    if not skip_indices:
        skip_indices = []
    n -= 1
    max_index, max_score = -1, -1
    for i, score in enumerate(lst):
        if i not in skip_indices and score > max_score:
            max_index, max_score = i, score
    if n == 0:
        return max_index, max_score
    else:
        skip_indices.append(max_index)
        return get_n_best_prediction(lst, n, skip_indices)


list1 = [1, 1, 1]
list2 = [1, 2]
list3 = [55, 66, 77, 88]
list4 = []

print(get_n_best_prediction(list1, 2))
print(get_n_best_prediction(list1, 1))
print(get_n_best_prediction(list2, 2))
print(get_n_best_prediction(list2, 1))
#print(get_n_best_prediction(list2, 3))
print(get_n_best_prediction(list3, 1))
print(get_n_best_prediction(list3, 2))
print(get_n_best_prediction(list3, 3))
print(get_n_best_prediction(list3, 4))
print(get_n_best_prediction(list4, 1))


exit()

input_tensor = torch.tensor([[[0.1, 0.3, 0.2]]], dtype=torch.float64)
print("Input size:")
print(input_tensor.size())

output_list = input_tensor.tolist()[0][0]
print("Output list:")
print(output_list)
