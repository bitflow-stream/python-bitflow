import sys
import time

from core.zerops.model_repository.BinaryModelRepository import BinaryModelRepository
from core.zerops.model_repository.BinaryModelWrapper import BinaryModelWrapper
from core.zerops.serialize.JSONSerializer import JSONSerializer

sleep_time = 3


class ExampleModel:
    def __init__(self, param1, param2):
        self.param1 = param1
        self.param2 = param2


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


if len(sys.argv) != 2:
    eprint("Need exactly 1 parameter: model key to use.")
    exit(1)

key = sys.argv[1]
serializer = JSONSerializer(ExampleModel)
repo = BinaryModelRepository(serializer, step_name="main")

i = 0

# while i < 5:
#     model = repo.load_latest(key)
#     if not model:
#         print("Model with key {} not yet stored. Creating...".format(key))
#         model = ExampleModel(i, "test_{}".format(i))
#         revision_number = repo.store(key, model)
#         print("Created new model with revision number: {}".format(revision_number))
#     else:
#         print("Loaded model: {}".format(model))
#
#     time.sleep(sleep_time)
#     i += 1
#     model = repo.load_latest(key)
#     model.param1 = i
#     model.param2 = "test_{}".format(i)
#     print("Storing updated model: {}".format(model))
#     revision_number = repo.store(key,  model)
#     print("Stored updated model with revision number: {}".format(revision_number))
#     time.sleep(sleep_time)

print(repo.get_all_latest_revisions('*'))
print(repo.load_all_latest_revisions('*'))
print(repo.load_revision(key, 0))

repo.close()
