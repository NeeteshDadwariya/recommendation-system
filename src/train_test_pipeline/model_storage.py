import os

ROOT_FOLDER = os.path.dirname(os.getcwd())
MODEL_DUMP_FOLDER = ROOT_FOLDER + '/model_dumps/'
MODEL_FILE_NAME = 'als_matrix_factorization.pkl'


def store_model(model):
    # Save the model as a pickle in a file
    model.write().overwrite().save(MODEL_DUMP_FOLDER + MODEL_FILE_NAME)
