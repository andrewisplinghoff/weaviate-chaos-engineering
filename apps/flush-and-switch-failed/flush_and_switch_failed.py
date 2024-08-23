"""
Tests that the tombstone cleanup completes successfully
"""

import random
import requests
import json
from re import U
import weaviate
from uuid import uuid3, NAMESPACE_DNS
from loguru import logger
import sys, shutil, os
import traceback
import datetime
import time
import itertools


def create_weaviate_schema(paragraph_collection_name):
    schema = {
        "classes": [
            {
                "class": paragraph_collection_name,
                "description": "A wiki paragraph",
                "vectorizer": "text2vec-contextionary",
                "vectorIndexConfig": {
                    "maxConnections": 1024,
                    "cleanupIntervalSeconds": 5,
                },
                "properties": [
                    {
                        "dataType": ["string"],
                        "description": "Title of the paragraph",
                        "name": "title",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        },
                    },
                    {
                        "dataType": ["text"],
                        "description": "The content of the paragraph",
                        "name": "content",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": False,
                                "vectorizePropertyName": False,
                            }
                        },
                    },
                    {
                        "dataType": ["text"],
                        "description": "The content of the paragraph",
                        "name": "stupid_long",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": False,
                                "vectorizePropertyName": False,
                            }
                        },
                    },
                    {
                        "dataType": ["int"],
                        "description": "Order of the paragraph",
                        "name": "order",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        },
                    },
                    {
                        "dataType": ["int"],
                        "description": "Number of characters in paragraph",
                        "name": "word_count",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        },
                    }
                ],
            },
        ]
    }
    # add schema
    if not client.schema.contains(schema):
        client.schema.create(schema)


def add_article_to_batch(parsed_line):
    return [
        {"title": parsed_line["title"]},
        "Article",
        str(uuid3(NAMESPACE_DNS, parsed_line["title"].replace(" ", "_"))),
    ]


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def add_paragraph_to_batch(parsed_line, paragraph_collection_name):
    return_array = []
    for paragraph in parsed_line["paragraphs"]:
        chunk_list = list(chunks(paragraph["content"], 825))
        for chunk_index, content in enumerate(chunk_list):
            add_object = {
                "content": content,
                "order": paragraph["count"],
                "word_count": len(content),
            }
            if "title" in paragraph:
                # Skip if wiki paragraph
                if ":" in paragraph["title"]:
                    continue
                add_object["title"] = paragraph["title"]
            # add to batch
            return_array.append(
                [
                    add_object,
                    paragraph_collection_name,
                    str(
                        uuid3(
                            NAMESPACE_DNS,
                            parsed_line["title"].replace(" ", "_")
                            + "___paragraph___"
                            + str(paragraph["count"])
                            + str(chunk_index)
                        )
                    ),
                ]
            )
    return return_array


def handle_results(results):
    if results is not None:
        for result in results:
            if (
                "result" in result
                and "errors" in result["result"]
                and "error" in result["result"]["errors"]
            ):
                for message in result["result"]["errors"]["error"]:
                    logger.debug(message["message"])


def import_data_without_crefs(wiki_data_file, paragraph_collection_name):
    counter = 1
    #counter_article = 0
    #counter_article_successful = 0
    counter_article_failed = 0
    counter_paragraph_successful = 0
    #uuids_a = []
    uuids_p = []
    uuids_ex = []
    with open(wiki_data_file) as f:
        for line in f:  # itertools.islice(f, 5):
            parsed_line = json.loads(line)
            if len(parsed_line["paragraphs"]) > 0:
                try:
                    article_obj = add_article_to_batch(parsed_line)
                    #counter_article += 1
                    # skip if it is a standard wiki category
                    if ":" in article_obj[2]:
                        continue
                    else:
                        #uuids_a.append(article_obj[2])
                        #if not client.data_object.exists(article_obj[2]):
                        #    # add the article obj
                        #    client.data_object.create(article_obj[0], article_obj[1], article_obj[2])
                        #    counter_article_successful += 1
                        #    counter += 1
                        paragraph_objs = add_paragraph_to_batch(parsed_line, paragraph_collection_name=paragraph_collection_name)
                        for paragraph_obj in paragraph_objs:
                            uuids_p.append(paragraph_obj[2])
                            if not client.data_object.exists(paragraph_obj[2]):
                                # add the paragraph obj
                                counter_paragraph_successful += 1
                                client.data_object.create(paragraph_obj[0], paragraph_obj[1], paragraph_obj[2])
                        #print(f'Added {len(paragraph_objs)=} to Weaviate')
                except Exception as e:
                    uuids_ex.append(article_obj[2])
                    counter_article_failed += 1
                    logger.error(f"issue adding article {article_obj[2]}")
                    logger.error(e)
                    logger.error("".join(traceback.format_tb(e.__traceback__)))
    logger.debug(
        f"paragraphs added {counter_paragraph_successful} / {counter_article_failed}"
    )
    client.batch.create_objects()
    client.batch.flush()
    return uuids_p, uuids_ex


def delete_uuids(uuids, collection_name):
    for uuid in uuids:
        client.batch.delete_objects(
            class_name=collection_name,
            where={
                "path": ["id"],
                "operator": "ContainsAny",
                "valueTextArray": uuids
            },
        )
    logger.info(f"deleted {collection_name} objects: {len(uuids)}")


def checkUUIDExists(_id: str):
    exists = client.data_object.exists(_id)
    if not exists:
        logger.error(f"ERROR!!! Object with ID: {_id} doesn't exist!!! exists: {exists}")
        raise


def checkIfObjectsExist(uuids):
    for _id in uuids:
        checkUUIDExists(_id)


def create_backup(client: weaviate.Client, name):
    res = client.backup.create(
        backup_id=name,
        backend='filesystem',
        wait_for_completion=True,
    )
    if res["status"] != "SUCCESS":
        fatal(f"Backup Create failed: {res}")


def checkForDuplicates(uuids):
    if len(set(uuids)) != len(uuids):
        logger.info("uuids contain duplicates")
    return set(uuids)


def unzip():
    shutil.unpack_archive("wikipedia1k.json.zip", ".")


def cleanup():
    os.remove("wikipedia1k.json")


def performImport(paragraph_collection_name):
    logger.info("Start import")
    wiki_data_file = "wikipedia1k.json"
    logger.info("Importing data")
    uuids_p, uuids_ex = import_data_without_crefs(wiki_data_file, paragraph_collection_name=paragraph_collection_name)
    #logger.info("Checking if objects exist articles")
    #uuids_a = checkForDuplicates(uuids_a)
    #checkIfObjectsExist(uuids_a)
    logger.info("Checking if objects exist paragraphs")
    uuids_p = checkForDuplicates(uuids_p)
    checkIfObjectsExist(uuids_p)
    return uuids_p


def get_metric(metric_name, class_name):
    metrics = requests.get("http://127.0.0.1:2112/metrics").text.split('\n')
    metrics = [el for el in metrics if el.startswith(metric_name)]
    metrics = [el for el in metrics if f'class_name="{class_name}"' in el]
    if len(metrics) != 1:
        raise ValueError(f"Didn't find exactly one metric for {metric_name=} {class_name=}, {metrics=}")
    metrics_line = metrics[0]
    parts = metrics_line.split(' ')
    if len(parts) != 2:
        raise ValueError(f"Metrics line doesn't looks as expected (key/value separated by a space) {parts=}, {metrics_line=}")
    value = parts[1]
    return int(value)


if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")
    try:
        unzip()
        random.seed(0)
        client.schema.delete_all()  # Clear up collections remaining from other runs
        # Creating an index
        paragraph_collection_name='Paragraph'
        create_weaviate_schema(paragraph_collection_name=paragraph_collection_name)
        try:
            # batch uploading around 30k objects(size with vector ~= 250 MB)
            uuids_p = performImport(paragraph_collection_name=paragraph_collection_name)

            # creating a backup
            backup_name = f"{int(datetime.datetime.now().timestamp())}"
            logger.info("Start backup")
            create_backup(client=client, name=backup_name)
            logger.info("Backup finished")

            #after 5 minutes retrieving the ids of objects based on a search(returns the ids of all objects)
            print('Sleeping for 5 minutes')
            time.sleep(5 * 60)
            response = client.query.get(paragraph_collection_name, []) \
                .with_additional(["id"]) \
                .with_limit(50_000) \
                .do()
            retrieved_obj_list = response['data']['Get'][paragraph_collection_name]
            print(f'{len(retrieved_obj_list)=}')
            
            # 5. deleting the objects with uuids using this code: collection.data.delete_many(where=Filter.by_id().contains_any(uuids_batch)) where the uuids_batch size is 5k
            logger.info("Deleting data")
            #delete_uuids(random.sample(list(uuids_a), int(random.randint(3,7)/10 * len(uuids_a))), "Articles")
            #delete_uuids(random.sample(list(uuids_p), int(random.randint(3,7)/10 * len(uuids_p))), "Paragraph")
            delete_uuids(random.sample(list(uuids_p), 5_000), "Paragraph")
            #if len(uuids_ex) > 0:
            #    delete_uuids(uuids_ex, "existing Articles")
            logger.info("Deletion done")

            # 6. batch uploading same data again to an empty index.
            create_weaviate_schema(paragraph_collection_name='Paragraph2')
            uuids_p_v2 = performImport(paragraph_collection_name='Paragraph2')

            # 7. backing up
            backup_name = f"{int(datetime.datetime.now().timestamp())}"
            logger.info("Start backup")
            create_backup(client=client, name=backup_name)
            logger.info("Backup finished")
            
            # Error should be caused by step 5.


            #paragraph_cleanup_threads = 0
            #retries = 0
            #logger.info('Waiting for tombstone cleanup cycle (paragraph_cleanup_threads > 0)')
            #while paragraph_cleanup_threads == 0 and get_metric(metric_name='vector_index_tombstone_cleaned', class_name='Paragraph') == 0:
            #    retries += 1
            #    if retries > 300:
            #        logger.error('Timeout waiting for paragraph tombstone cleanup to start')
            #        exit(1)
            #    paragraph_cleanup_threads = get_metric(metric_name='vector_index_tombstone_cleanup_threads', class_name='Paragraph')
            #    time.sleep(1)
            #logger.info('Tombstone cleanup started')
            
            #logger.info('Now waiting for tombstone cleanup to have finished (paragraph_cleanup_threads == 0)')
            #retries = 0
            #while paragraph_cleanup_threads > 0:
            #    retries += 1
            #    if retries > 60:
            #        logger.error('Timeout waiting for paragraph tombstone cleanup to finish')
            #        exit(1)
            #    paragraph_cleanup_threads = get_metric(metric_name='vector_index_tombstone_cleanup_threads', class_name='Paragraph')
            #    time.sleep(1)
            

           

            #logger.info('paragraph_cleanup_threads is zero')
            #vector_index_tombstones_article = get_metric(metric_name='vector_index_tombstones', class_name='Article')
            #vector_index_tombstones_paragraph = get_metric(metric_name='vector_index_tombstones', class_name='Paragraph')
            #logger.info(f'{vector_index_tombstones_article=} {vector_index_tombstones_paragraph=}')
            #retries = 0
            #logger.info('Wait for tombstones to be gone')
            #while vector_index_tombstones_article > 0 or vector_index_tombstones_paragraph > 0:
            #    retries += 1
            #    if retries > 300:
            #        if vector_index_tombstones_article > 0:
            #            logger.error(f'Article tombstone cleanup failed to clear up tombstones {vector_index_tombstones_article=} {vector_index_tombstones_paragraph=}')
            #            exit(1)
            #        if vector_index_tombstones_paragraph > 0:
            #            logger.error(f'Paragraph tombstone cleanup failed to clear up tombstones {vector_index_tombstones_paragraph=} {vector_index_tombstones_article=}')
            #            exit(1)
            #    vector_index_tombstones_article = get_metric(metric_name='vector_index_tombstones', class_name='Article')
            #    vector_index_tombstones_paragraph = get_metric(metric_name='vector_index_tombstones', class_name='Paragraph')
            #    time.sleep(1)
            #logger.info('All tombstones gone')
        except Exception as e:
            logger.exception("Exception occurred")
            exit(1)
        except:
            logger.exception("Exception occurred")
            exit(1)
    finally:
        cleanup()
