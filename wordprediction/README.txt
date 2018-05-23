▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --───────────────── ■  Release information   ■ ────────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
▀                                                                           ▀	
							"Word Prediction" Project 
							
Word Prediction project uses the 'Google N-gram' corpus in order to predict combination of words based on probability.

Authors:
Amir Avrahami 203204367
Orr Levy 203518766

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────── ■ Requirements for Word Prediction Project ■ ─────────- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
▀                                                                           ▀
[ 1 ] AWS Credentials file configured in ~.aws.credentials
[ 2 ] Region configured in ~.aws.config

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ ───────── ■    How to run Word Prediction program    ■ ────────── █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀

Run the following command in order to run the Word Prediction with the necessary information:
java -jar EMRControl.jar <EMRClusterId> <S3OutputPath>

EMRControl expects EMRClusterID as part of its arguments, an EMR cluster should be up and running manually.

EMRControl includes hard-coded paths to the input files (Google N-gram):
[ 1 ] s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data
[ 2 ] s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data
[ 3 ] s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■         Running results          ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
S3 Output: https://s3.amazonaws.com/wordprediction216493892236/OutputWithCombiner/
