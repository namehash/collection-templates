.PHONY: all

all: data/merged_final.jsonl

#TODO everything should be unquoted because some characters are not escaped properly, e.g. comma
TIME_CMD=/bin/time -v -o pipeline.times -a

filter: data/latest-all.nt.bz2.filtered.bz2 data/stats_predicates.txt data/stats_instance_of.txt

stats: data/stats_predicates.txt data/stats_instance_of.txt

#data/latest-all.nt.bz2:
#	${TIME_CMD} wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.nt.bz2 -O $@

#TODO replace `bzcat` with `lbzip2 -d -c` to make it faster
data/latest-all.nt.bz2.filtered.bz2:
	${TIME_CMD} wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.nt.bz2 -O - | bzcat | grep -E '^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))' | pbzip2 -c > $@
# latest-all.nt.bz2.filtered.bz2 3,5GB
# latest-all.nt.bz2.filtered 50GB

#old: data/latest-all.nt.bz2.filtered.bz2: data/latest-all.nt.bz2
#	old: ${TIME_CMD} pv $< | bzcat | grep -E '^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))' | pbzip2 -c > $@

	
data/stats_predicates.txt: data/latest-all.nt.bz2.filtered.bz2
	pv $< | pbzip2 -d -c | cut -d ' ' -f 2 | sort | uniq -c > $@
	cat $@

data/stats_instance_of.txt: data/latest-all.nt.bz2.filtered.bz2
	pv $< | pbzip2 -d -c | grep "<http://www\.wikidata\.org/prop/direct/P31>" | cut -d ' ' -f 3 | sort | uniq -c | sort -nr > $@
	head -n 20 $@

data/db1.rocks: data/latest-all.nt.bz2.filtered.bz2 # 1.5h
	${TIME_CMD} python scripts/create_kv.py $<
#MULTI OUTPUT: data/db2.rocks data/db3.rocks data/db4.rocks data/db5.rocks data/db6.rocks

data/db1_rev.rocks: data/db1.rocks
	${TIME_CMD} python scripts/reverse_rocksdict.py data/db1.rocks/ data/db1_rev.rocks/

########################  PREPARING VALID LISTS AND CATEGORIES  ########################
prepare_lists_and_categories: data/categories.json data/lists.json

#INPUT: data/db3.rocks
data/categories.json: data/db1_rev.rocks
	${TIME_CMD} python scripts/create_lists.py $@ --mode category
# output:
#  {
#    "item": "Q100088400",
#    "type": [
#      "Q5"
#    ],
#    "article": "Category:Writers_from_Z%C3%BCrich"
#  },	

data/lists.json: data/db1_rev.rocks
	${TIME_CMD} python scripts/create_lists.py $@ --mode list
# output:
#   {
#    "item": "Q100673110",
#    "type": [
#      "Q11424"
#    ],
#    "article": "List_of_Walt_Disney_Studios_films_(2000%E2%80%932009)"
#  },

########################  DOWNLOADING MEMBERS  ########################
download_members: download_list_members download_category_members

############## LIST MEMBERS ##############
# FIXME currently parser expects the gz to have date in it (think about PR to fix that)
data/enwiki-20230401-pagelinks.sql.gz:
	${TIME_CMD} wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pagelinks.sql.gz -O $@

data/allowed-lists.txt: data/lists.json data/index_enwiki-latest.db
	${TIME_CMD} python scripts/extract_allowed_lists.py $^ $@
# output is a list of page ids:
# 23455140

data/enwiki-pagelinks.csv: data/enwiki-20230401-pagelinks.sql.gz data/allowed-lists.txt
	${TIME_CMD} python scripts/parse_wiki_dump.py $< $@ --mode list --allowed_values data/allowed-lists.txt
# 126m, output:
# 23455140,1.FC_Nürnberg
# 33030326,1.FC_Nürnberg

data/mapped-lists.csv: data/enwiki-pagelinks.csv data/index_enwiki-latest.db
	${TIME_CMD} python scripts/map_to_wikidata_ids_and_titles.py $^ $@ --mode list
# output:
# Q6620950,1.FC_Nürnberg
# Q6589143,1.FC_Nürnberg
# List_of_footballers_killed_during_World_War_II,1.FC_Nürnberg
# List_of_Malaysian_football_transfers_2012,1.FC_Nürnberg
# Swedish_women's_football_clubs_in_international_competitions,1.FFC_Frankfurt

data/sorted-lists.csv: data/mapped-lists.csv
	(head -n 1 $< && tail -n +2 $< | LC_ALL=C sort) > $@
# 1954_FIFA_World_Cup_squads,1._FC_Nürnberg

data/list_links.jsonl: data/sorted-lists.csv data/lists.json
	${TIME_CMD} python scripts/reformat_csv_to_json.py $< $@ --list_of_collections data/lists.json #--mode list
# output
#     {"item": "Q1000775", "type": ["Q11446"], "article": "SMS_W%C3%BCrttemberg", "members": ["SMS Württemberg (1878)", "Bayern-class battleship", "Kaiserliche Marine", "SMS Württemberg",  "SMS Württemberg (1917)", "Sachsen-class ironclad", "WikiProject Ships/Guidelines"]}
# old:{"item": "Q1000775", "type": ["Q11446"], "article": "SMS_W%C3%BCrttemberg", "members": ["SMS Württemberg (1878)", "Bayern-class battleship", "Kaiserliche Marine", "Sachsen-class ironclad", "SMS Württemberg (1917)"]}

download_list_members: data/list_links.jsonl

############## CATEGORY MEMBERS ##############
# FIXME currently parser expects the gz to have date in it (think about PR to fix that)
data/enwiki-20230401-categorylinks.sql.gz:
	${TIME_CMD} wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-categorylinks.sql.gz -O $@
# 143237,'Writers_from_Zürich',...

data/allowed-categories.txt: data/categories.json
	${TIME_CMD} python scripts/extract_allowed_categories.py $< $@
# output is a list of category titles:
# Category:Writers_from_Z%C3%BCrich

data/enwiki-categories.csv: data/enwiki-20230401-categorylinks.sql.gz data/allowed-categories.txt
	${TIME_CMD} python scripts/parse_wiki_dump.py $< $@ --mode category --allowed_values data/allowed-categories.txt
# 33m: output
# 143237,Writers_from_Zürich

data/mapped-categories.csv: data/enwiki-categories.csv data/index_enwiki-latest.db data/categories.json
	${TIME_CMD} python scripts/map_to_wikidata_ids_and_titles.py $< data/index_enwiki-latest.db $@ --mode category --categories data/categories.json
# 3m, output:
# Q8879623,Park_Güell
# Writers_from_Zürich,Johann_Georg_Baiter
# Antoni_Gaudí_buildings,Park_Güell

data/sorted-categories.csv: data/mapped-categories.csv
	(head -n 1 $< && tail -n +2 $< | LC_ALL=C sort) > $@

data/category_members.jsonl: data/sorted-categories.csv data/categories.json
	${TIME_CMD} python scripts/reformat_csv_to_json.py $< $@ --list_of_collections data/categories.json #--mode category
# output:
# {"item": "Q100088400", "type": ["Q5"], "article": "Category:Writers_from_Z%C3%BCrich", "members": ["Alain de Botton", "Annemarie Schwarzenbach", "Arnold Kübler", "Bernhard Diebold", "Bruno Barbatti", "Carl Seelig", "Charles Lewinsky", "Conrad Ferdinand Meyer", "Egon von Vietinghoff", "Elisabeth Joris", "Esther Dyson", "Fleur Jaeggy", "Gerold Meyer von Knonau (1804–1858)", "Gottfried Keller", "Gotthard Jedlicka", "Hans-Ulrich Indermaur", "Hugo Loetscher", "Ilma Rakusa", "Johann Caspar Scheuchzer", "Johann Georg Baiter", "Johann Jakob Breitinger", "Johann Jakob Hottinger (historian)", "Johann Kaspar Lavater", "Jürg Schubiger", "Ludwig Hirzel (historian)", "Mariella Mehr", "Markus Hediger", "Max Frisch", "Max Rychner", "Moustafa Bayoumi", "Olga Plümacher", "Peter Zeindler", "Robert Faesi", "Roger Sablonier", "Stefan Maechler", "Taya Zinkin", "Verena Conzett", "Werner Vordtriede", "Wilhelm Wartmann"]}

download_category_members: data/category_members.jsonl
	
########################  WIKIMAPPER SETUP  ########################
#wikimapper: data/index_enwiki-latest.db

data/enwiki-latest-redirect.sql.gz:
	${TIME_CMD} wikimapper download enwiki-latest --dir data

data/index_enwiki-latest.db: data/enwiki-latest-redirect.sql.gz
	${TIME_CMD} wikimapper create enwiki-latest --dumpdir data --target $@
	
# wikimapper stores: wikipedia_id, wikipedia_title, wikidata_id
# also redirects, for which wikidata_id overriden by target aricle

########################  QRANK  ########################
#qrank: data/qrank.csv

data/qrank.csv:
	${TIME_CMD} wget -O - https://qrank.wmcloud.org/download/qrank.csv.gz | gunzip -c > $@

########################  ???  ########################

# filter members of lists and categories
# Wikidata redirects

#TODO pre compute force_normalize function and interestin_score in parallel

cache_interesting_score: cache_interesting_score_lists cache_interesting_score_categories

cache1: data/validated_list_links.jsonl
	${TIME_CMD} python scripts/cache_interesting_score_local.py $< -n 111000 && touch cache1

cache2: data/validated_category_members.jsonl cache1
	${TIME_CMD} python scripts/cache_interesting_score_local.py $< -n 460000 && touch cache2

########################  VALIDATE TYPES  ########################

#INPUT: data/db1.rocks data/db2.rocks data/db6.rocks data/index_enwiki-latest.db
data/validated_list_links.jsonl: data/list_links.jsonl
	${TIME_CMD} python3 scripts/filter_articles2.py $< $@ -n 111000
# 29:08<00:33, 62.30it/s
#Members 7052484 valid, 22489645 invalid
#No parent 1308946
#No parent 642624
#but should be Members 7.057.739 valid, 25.985.431 invalid

# Members 5275966 valid, 19635289 invalid   
# No parent 1210190 

#INPUT: data/db1.rocks data/db2.rocks data/db6.rocks data/index_enwiki-latest.db
data/validated_category_members.jsonl: data/category_members.jsonl
	${TIME_CMD} python3 scripts/filter_articles2.py $< $@ -n 460000
# 461543it [22:38, 339.84it/s]
#Members 20456859 valid, 8641896 invalid
#No parent 1358593
#should be Members 21.294.548 valid, 7.888.585 invalid

# Members 20514855 valid, 8576122 invalid
#No parent 1364458
#
#real    18m43,154s
#user    16m22,530s
#sys     2m10,075s

#INPUT: data/db5.rocks data/index_enwiki-latest.db
data/category_members_all_info.jsonl: data/validated_category_members.jsonl cache2 data/suggestable_domains.csv data/qrank.csv
	${TIME_CMD} python3 scripts/prepare_members_names.py $< data/qrank.csv $@ -n 460000

#INPUT: data/db5.rocks data/index_enwiki-latest.db
data/list_links_all_info.jsonl: data/validated_list_links.jsonl cache2 data/suggestable_domains.csv data/qrank.csv
	${TIME_CMD} python3 scripts/prepare_members_names.py $< data/qrank.csv $@ -n 111000
	

#${TIME_CMD} python3 scripts/prepare_collections2.py data/list_links_all_info.jsonl data/list_links_final.jsonl -n 111000

#${TIME_CMD} python3 scripts/prepare_collections2.py data/category_members_all_info.jsonl data/category_members_final.jsonl -n 460000

#INPUT: data/db4.rocks
data/merged.jsonl: data/list_links_all_info.jsonl data/category_members_all_info.jsonl
	${TIME_CMD} python scripts/merge_lists_and_categories.py data/list_links_all_info.jsonl data/category_members_all_info.jsonl data/merged.jsonl

# All collections: 570487
#Lists: 108944, Categories: 461543, Written 511932
#Merged by type 6996 categories into lists
#Merged by name 6720 categories into lists
#Filtered by type: 44096
#Filtered by prefix: 743


#All collections: 570487
#Lists: 108944, Categories: 461543, Written 503427
#Merged by type 6920 categories into lists
#Merged by name 6712 categories into lists
#Filtered by type: 44096
#Filtered by prefix: 743
#Filtered by by: 8589

#All collections: 522965
#Lists: 103315, Categories: 419650, Written 480519
#Merged by type 7314 categories into lists
#Merged by name 7437 categories into lists
#Filtered by type: 24386
#Filtered by prefix: 0
#Filtered by by: 3309

data/merged_filtered.jsonl: data/merged.jsonl
	${TIME_CMD} python scripts/merge_collections_ending_with_letters.py data/merged.jsonl data/merged_filtered.jsonl -n 503427
#Matches: 3554
#Merged: 3462

#Matches: 5728
#Merged: 4990
#Matches: 5712
#Merged: 5330
data/merged_filtered_dup.jsonl: data/merged_filtered.jsonl
	${TIME_CMD} python scripts/filter_duplicates.py data/merged_filtered.jsonl data/merged_filtered_dup.jsonl -n 500139
# Merged: 261
# Merged: 16848
# Merged: 16870

data/merged_final.jsonl: data/merged_filtered_dup.jsonl
	${TIME_CMD} python3 scripts/prepare_collections2.py data/merged_filtered_dup.jsonl data/merged_final.jsonl -n 500008
	
# finally 419030
# finally 411776
#356.40user 17.38system 6:35.02elapsed 94%CPU (0avgtext+0avgdata 3853560maxresident)k
#7487120inputs+17323992outputs (2major+2573649minor)pagefaults 0swaps