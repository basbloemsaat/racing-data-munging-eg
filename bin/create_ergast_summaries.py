#!/usr/bin/env python

from rdmdb import exec_query, save_to_json


from pprint import pprint
from dumper import dump

seasons = exec_query(
    query='''
        SELECT 
            ra.year
            , count(DISTINCT d.driverId) as winners
            , count(DISTINCT ra.raceId) as races
            , s.url
        FROM 
            seasons s 
            JOIN races ra ON ra.year=s.year
            LEFT JOIN results re ON re.raceId = ra.raceId AND re.position=1
            LEFT JOIN drivers d ON d.driverId = re.driverId
        GROUP BY ra.year
        ORDER BY ra.year
    ''',
    json_file='seasons.json'
)
