## Libraries: 

`pip3 install numpy, pandas, sklearn` 

## Run parallel knn:

First you need to train:  

`python3 parallel_knn_train.py -t <train-dataset.csv> -m <path-to-store-model> -maxK <maximumK-for-training> -seed <seed-for-kfolds>`

After training model ready, you can evaluate: 

`python3 parallel_knn_predict.py -m <path-to-store-model> -e <eval-dataset.csv> -o <output-data>`  