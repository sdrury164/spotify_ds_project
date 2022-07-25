# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 10:45:00 2022

@author: Steve

Much of the following is adapted from the gensim LDA tutorial:
(https://radimrehurek.com/gensim/auto_examples/tutorials/run_lda.html#sphx-glr-auto-examples-tutorials-run-lda-py).
    
"""

import sqlite3 as sql

import os
from datetime import datetime

import p4k_db_functions as db_func

from nltk.tokenize import RegexpTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from gensim.models import Phrases
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from gensim import corpora

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

current_dt_str = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')

conn = sql.connect('p4k_review_features.db')

def before_lda_prep():
    col_dict = db_func.get_col_dict()
    
    lyrics = db_func.get_df_from_table_name(conn, 'LYRICS', col_dict['lyrics'])
    
    album_lyrics = lyrics.groupby('album_id')['lyrics'].apply(list).reset_index()
    
    album_lyrics['joined_lyrics'] = [' '.join(lyr) for lyr in album_lyrics['lyrics']]
    
    # The following is adapted from the gensim LDA tutorial
    # (https://radimrehurek.com/gensim/auto_examples/tutorials/run_lda.html#sphx-glr-auto-examples-tutorials-run-lda-py).
    
    docs = list(album_lyrics['joined_lyrics'])
    
    # Split the document into single words (tokens)
    tokenizer = RegexpTokenizer(r'\w+')
    
    print('Tokenizing docs...')
    
    for i in range(len(docs)):
        docs[i] = docs[i].lower()
        docs[i] = tokenizer.tokenize(docs[i])
        
    # Remove single-character words (they don't tell us much about topics)
    docs = [[token for token in doc if len(token) > 1] for doc in docs]
    
    print('Docs tokenized.')
    
    # Lemmatize documents (map multiple tokens to single words that represent their meaning)
    
    print('Lemmatizing docs...')
    
    lemmatizer = WordNetLemmatizer()
    docs = [[lemmatizer.lemmatize(token) for token in doc] for doc in docs]
    
    print('Docs lemmatized.')
    
    # Find bigrams and trigrams
    print('Finding bigrams and trigrams...')
    
    # min_count: minimum number of bigrams/trigrams in docs to not ignore
    bigram = Phrases(docs, min_count=10)
    for i in range(len(docs)):
        for token in bigram[docs[i]]:
            if '_' in token: # ...then token is a bigram
                docs[i].append(token)
                
    print('Found bigrams and trigrams.')
    
    # Create a dictionary from documents
    # Then filter out words based on frequency of occurence
    
    print('Creating dictionary...')
    
    dictionary = Dictionary(docs)
    dictionary.filter_extremes(no_below=20, no_above=0.4)
    
    print('Created dictionary.')
    print(' Saving dictionary...')
    
    dictionary.save('lyrics_dictionary_' + current_dt_str + '.dict')
    
    print('Dictionary saved.')
    
    # Vectorize docs into bag-of-words representation
    
    print('Creating corpus...')
    
    corpus = [dictionary.doc2bow(doc) for doc in docs]
    
    print('Created corpus.')
    print('Saving corpus...')
    
    corpus_filename = 'lyrics_corpus_' + current_dt_str + '.mm'
    corpora.MmCorpus.serialize(corpus_filename, corpus)
    
    print('Corpus saved.')
    
    # Print some info about tokens and documents
    print('\nNumber of unique tokens = ' + str(len(dictionary)))
    print('Number of documents = ' + str(len(corpus)))
    
    temp = dictionary[0] # This loads the dictionary
    id2word = dictionary.id2token
    
    return_dict = {
        'corpus': corpus,
        'dictionary': dictionary,
        'id2word': id2word
    }
    
    return return_dict


def train_save_lda(chunksize, iterations, num_topics, passes, corpus=None, id2word=None, dictionary=None, eval_every=None, new_prep=False):
    if new_prep == True:
        prep_dict = before_lda_prep()
        corpus = prep_dict['corpus']
        dictionary = prep_dict['dictionary']
        id2word = prep_dict['id2word']
    elif not id2word:
        if dictionary:
            id2word = dictionary.id2token
        else:
            print('If no id2word is passed in, a dictionary must be passed in to create one. Returning None.')
            return None
    elif not corpus and not id2word and not dictionary:
        print('Need to specify new_prep = True if not passing in corpus, id2word, and dictionary. Returning None.')
        return None
    # Set up function to train LDA model for different parameters
    model = LdaModel(
        corpus=corpus,
        id2word=id2word,
        chunksize=chunksize,
        alpha='auto',
        eta='auto',
        iterations=iterations,
        num_topics=num_topics,
        passes=passes,
        eval_every=eval_every
    )
    filename = 'lda_model_t' + str(num_topics) + '_c' + str(chunksize) + '_p' + str(passes) + '_i' + str(iterations)
    model.save(filename)
    return model

def load_dict_for_lda(corpus_filename, dictionary_filename):
    lda_dict = dict()
    lda_dict['corpus'] = corpora.MmCorpus(corpus_filename)
    lda_dict['dictionary'] = Dictionary.load(dictionary_filename)
    lda_dict['id2word'] = lda_dict['dictionary'].id2token
    return lda_dict
