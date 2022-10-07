import json
import psycopg2
import time
import datetime
import csv

def write_authors():
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()

    try:
        start = time.time()
        with open("authors.jsonl") as file:
            list_to_insert = []
            for iterator, line in enumerate(file):

                id_to_insert = (json.loads(line))["id"]
                name_to_insert = (json.loads(line))["name"].replace("\x00", "\uFFFD")
                username_to_insert = (json.loads(line))["username"].replace("\x00", "\uFFFD")
                followers_count_to_insert = (json.loads(line))["public_metrics"]["followers_count"]
                following_count_to_insert = (json.loads(line))["public_metrics"]["following_count"]
                tweet_count_to_insert = (json.loads(line))["public_metrics"]["tweet_count"]
                listed_count_to_insert = (json.loads(line))["public_metrics"]["listed_count"]
                description_to_insert = (json.loads(line))["description"].replace("\x00", "\uFFFD")

                list_to_insert.append((id_to_insert, name_to_insert, username_to_insert, followers_count_to_insert,
                                       following_count_to_insert, tweet_count_to_insert, listed_count_to_insert,
                                       description_to_insert))

                if iterator % 10000 == 0:
                    args_str = ','.join(
                        cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", element).decode("utf-8") for element in
                        list_to_insert)
                    cursor.execute('INSERT INTO authors VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                    connection.commit()
                    list_to_insert.clear()
                    print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")
                    start = time.time()

            if list_to_insert:
                args_str = ','.join(
                    cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", element).decode("utf-8") for element in list_to_insert)
                cursor.execute('INSERT INTO authors VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                connection.commit()
                list_to_insert.clear()
                print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


def write_conversations():
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()

    try:
        start = time.time()
        with open("conversations.jsonl") as file:
            conversations_list = []
            authors_ids_list = []
            hashtags_list = []
            domains_list = []
            entities_list = []

            for iterator, line in enumerate(file):

                # Conversations
                id_to_insert = (json.loads(line))["id"]
                content_insert = (json.loads(line))["text"]
                possibly_sensitive_to_insert = (json.loads(line))["possibly_sensitive"]
                language_to_insert = (json.loads(line))["lang"]
                source_to_insert = (json.loads(line))["source"]
                retweet_count_to_insert = (json.loads(line))["public_metrics"]["retweet_count"]
                reply_count_to_insert = (json.loads(line))["public_metrics"]["reply_count"]
                like_count_to_insert = (json.loads(line))["public_metrics"]["like_count"]
                quote_count_to_insert = (json.loads(line))["public_metrics"]["quote_count"]
                created_at = (json.loads(line))["created_at"]
                author_id_to_insert = (json.loads(line))["author_id"]

                conversations_list.append(
                    (id_to_insert, content_insert, possibly_sensitive_to_insert, language_to_insert,
                     source_to_insert, retweet_count_to_insert, reply_count_to_insert, like_count_to_insert,
                     quote_count_to_insert, created_at, author_id_to_insert))
                authors_ids_list.append((author_id_to_insert,))

                # Hashtags
                try:
                    for tag in (json.loads(line))["entities"]["hashtags"]:
                        tag_to_insert = (tag["tag"])
                        hashtags_list.append((tag_to_insert,))
                except Exception:
                    pass

                # Domains
                try:
                    for domain in (json.loads(line))["context_annotations"]:
                        domain_id = domain["domain"]["id"]
                        domain_name = domain["domain"]["name"]
                        domain_description = domain["domain"]["description"]
                        domains_list.append((domain_id, domain_name, domain_description))
                except Exception:
                    pass

                # Entities
                try:
                    for entity in (json.loads(line))["context_annotations"]:
                        entity_id = entity["entity"]["id"]
                        entity_name = entity["entity"]["name"]
                        entity_description = entity["entity"]["description"]
                        entities_list.append((entity_id, entity_name, entity_description))
                except Exception:
                    pass

                if iterator % 1000 == 0:
                    args_str = ','.join(cursor.mogrify("(%s)", element).decode("utf-8") for element in authors_ids_list)
                    cursor.execute('INSERT INTO authors VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )

                    args_str = ','.join(
                        cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", element).decode("utf-8") for element in
                        conversations_list)
                    cursor.execute('INSERT INTO conversations VALUES {0} ON CONFLICT(id) DO NOTHING'.format(args_str), )

                    if domains_list:
                        args_str = ','.join(
                            cursor.mogrify("(%s,%s,%s)", element).decode("utf-8") for element in domains_list)
                        cursor.execute(
                            'INSERT INTO context_domains VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                        domains_list.clear()

                    if entities_list:
                        args_str = ','.join(
                            cursor.mogrify("(%s,%s,%s)", element).decode("utf-8") for element in entities_list)
                        cursor.execute(
                            'INSERT INTO context_entities VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                        entities_list.clear()

                    if hashtags_list:
                        args_str = ','.join(
                            cursor.mogrify("(default,%s)", element).decode("utf-8") for element in hashtags_list)
                        cursor.execute('INSERT INTO hashtags VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                        # print("here")

                    connection.commit()
                    conversations_list.clear()
                    authors_ids_list.clear()
                    hashtags_list.clear()
                    print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")
                    start = time.time()

            if authors_ids_list:
                args_str = ','.join(cursor.mogrify("(%s)", element).decode("utf-8") for element in authors_ids_list)
                cursor.execute('INSERT INTO authors VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )

            if conversations_list:
                args_str = ','.join(
                    cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", element).decode("utf-8") for element in
                    conversations_list)
                cursor.execute('INSERT INTO conversations VALUES {0} ON CONFLICT(id) DO NOTHING'.format(args_str), )
                connection.commit()
                conversations_list.clear()

            if domains_list:
                args_str = ','.join(cursor.mogrify("(%s,%s,%s)", element).decode("utf-8") for element in domains_list)
                cursor.execute('INSERT INTO context_domains VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                domains_list.clear()

            if entities_list:
                args_str = ','.join(cursor.mogrify("(%s,%s,%s)", element).decode("utf-8") for element in entities_list)
                cursor.execute('INSERT INTO context_entities VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                entities_list.clear()

            if hashtags_list:
                args_str = ','.join(
                    cursor.mogrify("(default,%s)", element).decode("utf-8") for element in hashtags_list)
                cursor.execute('INSERT INTO hashtags VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                # print("here")

            connection.commit()
            conversations_list.clear()
            authors_ids_list.clear()
            hashtags_list.clear()
            print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


def write_other():
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()

    try:
        start = time.time()
        with open("conversations.jsonl") as file:
            annotations_list = []
            links_list = []
            context_annotations_list = []

            for iterator, line in enumerate(file):

                try:
                    for annotation in (json.loads(line))["entities"]["annotations"]:
                        annotations_list.append(((json.loads(line))['id'], annotation['normalized_text'],
                                                 annotation['type'], annotation['probability']))
                except Exception:
                    pass

                try:
                    for link in (json.loads(line))["entities"]["urls"]:
                        if len(link['expanded_url']) <= 2048:
                            links_list.append(((json.loads(line))['id'], link['expanded_url'],
                                               (link['title'] if 'title' in link else ""),
                                               (link['description'] if 'description' in link else "")))
                except Exception:
                    pass

                try:
                    for domain in (json.loads(line))["context_annotations"]:
                        domain_id = domain["domain"]["id"]
                    try:
                        for entity in (json.loads(line))["context_annotations"]:
                            entity_id = entity["entity"]["id"]
                            context_annotations_list.append(((json.loads(line))['id'], domain_id, entity_id))
                    except Exception:
                        pass
                except Exception:
                    pass

                if iterator % 10000 == 0:
                    if annotations_list:
                        args_str = ','.join(
                            cursor.mogrify('(default,%s,%s,%s,%s)', i).decode('utf-8') for i in annotations_list)
                        cursor.execute('INSERT INTO annotations VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                    if links_list:
                        args_str = ','.join(
                            cursor.mogrify('(default,%s,%s,%s,%s)', i).decode('utf-8') for i in links_list)
                        cursor.execute('INSERT INTO links VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )

                    connection.commit()
                    annotations_list.clear()
                    links_list.clear()
                    print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")
                    start = time.time()

                    # if context_annotations_list:
                    #     args_str = ','.join(cursor.mogrify('(default,%s,%s,%s)', i).decode('utf-8') for i in context_annotations_list)
                    #     cursor.execute('INSERT INTO context_annotations VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
                    #     connection.commit()
                    #     context_annotations_list.clear()

            if annotations_list:
                args_str = ','.join(
                    cursor.mogrify('(default,%s,%s,%s,%s)', i).decode('utf-8') for i in annotations_list)
                cursor.execute('INSERT INTO annotations VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )
            if links_list:
                args_str = ','.join(cursor.mogrify('(default,%s,%s,%s,%s)', i).decode('utf-8') for i in links_list)
                cursor.execute('INSERT INTO links VALUES {0} ON CONFLICT DO NOTHING'.format(args_str), )

            connection.commit()
            annotations_list.clear()
            links_list.clear()
            print("Total time:", time.time() - total, "s | 10k block:", time.time() - start, "s")


    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


total = time.time()
write_authors()
write_conversations()
write_other()
