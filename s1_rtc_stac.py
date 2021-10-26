#!/usr/bin/env python3
'''
Generate example STAC from test data
'''
import concurrent.futures
import s3fs
import sys
import pystac
import os
from stactools.sentinel1.rtc.stac import create_item, create_collection

print(os.environ)

MGRS = sys.argv[1]
fs = s3fs.S3FileSystem(anon=True)

def s3_to_http(s3path, region='us-west-2'):
    s3prefix = 'sentinel-s1-rtc-indigo'
    newprefix = f'https://sentinel-s1-rtc-indigo.s3.{region}.amazonaws.com'
    http = s3path.replace(s3prefix, newprefix)
    return http

def get_paths(zone=12, latLabel='S', square='YJ', year='*', date='*'):
    bucket = 'sentinel-s1-rtc-indigo'
    s3Path = f'{bucket}/tiles/RTC/1/IW/{zone}/{latLabel}/{square}/{year}/{date}/'
    print(f'searching {s3Path}...')
    keys = fs.glob(s3Path)
    print(f'{len(keys)} images matching {s3Path}')
    hrefs = [s3_to_http(x) for x in keys]
    return hrefs

if __name__ == '__main__':
    paths = get_paths(zone=MGRS[:2], latLabel=MGRS[2], square=MGRS[3:], year=2020)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        items = executor.map(create_item, paths)

    catalog = pystac.Catalog(id='aws-rtc-stac',
                             description='https://github.com/relativeorbit/aws-rtc-stac')
    collection = create_collection()
    collection.add_items(items)
    catalog.add_child(collection)
    catalog.generate_subcatalogs(template='${sentinel:mgrs}/${year}')
    catalog.normalize_hrefs('./')
    catalog.validate()
    catalog.save(catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)
