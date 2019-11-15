#!/usr/bin/env python
# coding: utf-8

# In[27]:


import json
import os
import pathlib
import time

import numpy as np
import rasterio
from rasterio.plot import show
import requests
from requests.auth import HTTPBasicAuth

# The 8 sensor locations used for this research.
CAMS = [0,1,2,3,4,5,6,7];
CAMS[0] = ['1604', -97.6649360, 29.8649170];
CAMS[1] = ['6602', -97.5417940, 30.5457060];
CAMS[2] = ['1612', -97.2937410, 30.1082220];
CAMS[3] = ['0614', -98.0833473, 30.2146162];
CAMS[4] = ['1613', -97.3785080, 30.3478080];
CAMS[5] = ['1675', -97.9288560, 29.8622810];
CAMS[6] = ['1603', -97.8937440, 30.2163970];
CAMS[7] = ['0690', -97.7345790, 30.6664421];

# API Key stored as an env variable
PLANET_API_KEY = os.getenv('PL_API_KEY')

# Search parameters
## The type of band procured.
item_type = "PSScene4Band"
## The radius of the search box in decimal degrees.
size = .05;
## The year of the search.
year = 2018;
## The maximum amount of cloud cover allowed per image.
cloud_cover_lte = .01;
## Used for Image IDs
same_src_products = [0];


# In[93]:


# define helpful functions for submitting, polling, and downloading an order
def place_order(request, auth):
    response = requests.post(orders_url, data=json.dumps(request), auth=auth, headers=headers)
    print(response)
    
    if not response.ok:
        raise Exception(response.content)

    order_id = response.json()['id']
    print(order_id)
    order_url = orders_url + '/' + order_id
    return order_url

def poll_for_success(order_url, auth, num_loops=50):
    count = 0
    while(count < num_loops):
        count += 1
        r = requests.get(order_url, auth=auth)
        response = r.json()
        state = response['state']
        print(state)
        success_states = ['success', 'partial']
        if state == 'failed':
            raise Exception(response)
        elif state in success_states:
            break
        
        time.sleep(10)
        
def download_order(order_url, auth, overwrite=False):
    r = requests.get(order_url, auth=auth)
    print(r)
    response = r.json()
    print(response)
    results = response['_links']['results']
    results_urls = [r['location'] for r in results]
    results_names = [r['name'] for r in results]
    results_paths = [pathlib.Path(os.path.join('data', n)) for n in results_names]
    print('{} items to download'.format(len(results_urls)))
    
    for url, name, path in zip(results_urls, results_names, results_paths):
        if overwrite or not path.exists():
            print('downloading {} to {}'.format(name, path))
            r = requests.get(url, allow_redirects=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            open(path, 'wb').write(r.content)
        else:
            print('{} already exists, skipping {}'.format(path, name))
            
    return dict(zip(results_names, results_paths))

# define helpful functions for visualizing downloaded imagery
def show_rgb(img_file):
    with rasterio.open(img_file) as src:
        b,g,r,n = src.read()

    rgb = np.stack((r,g,b), axis=0)
    show(rgb/rgb.max())
    
def show_gray(img_file):
    with rasterio.open(img_file) as src:
        g = src.read(1)
    show(g/g.max())


# In[28]:


# This function takes coordinates and a size (in degrees) to create a geojson bounding box.
def get_bounding_box(coordinates, size):
    # Bounding Box around a CAMS.
    geojson_geometry = {
      "type": "Polygon",
      "coordinates": [
        [ 
          [coordinates[1] - size, coordinates[2] + size],
          [coordinates[1] + size, coordinates[2] + size],
          [coordinates[1] + size, coordinates[2] - size],
          [coordinates[1] - size, coordinates[2] - size],
          [coordinates[1] - size, coordinates[2] + size]
        ]
      ]
    }
    return geojson_geometry

# This function takes the Area of Interest (AOI), year, month, and desired cloud cover parameter to create a combined filter. 
def create_filter(AOI, year, month, lte_clouds):
    geometry_filter = {
      "type": "GeometryFilter",
      "field_name": "geometry",
      "config": AOI
    }

    if month <= 9:
        gte_date = str(year) + '-0' + str(month) + '-01T00:00:00.000Z';
        if month == 9:
            lte_date = str(year) + '-' + str(month+1) + '-01T00:00:00.000Z';
        else:
            lte_date = str(year) + '-0' + str(month+1) + '-01T00:00:00.000Z';
    elif month > 9 & i <= 99:
        gte_date = str(year) + '-' + str(month) + '-01T00:00:00.000Z';
        if month == 12:
            lte_date = str(year + 1) + '-01-01T00:00:00.000Z';
        else:
            lte_date = str(year) + "-" + str(month+1) + '-01T00:00:00.000Z';
    
    # get images acquired within a date range
    date_range_filter = {
      "type": "DateRangeFilter",
      "field_name": "acquired",
      "config": {
        "gte": gte_date,
        "lte": lte_date
      }
    }

    # only get images which have less than 1% cloud coverage
    cloud_cover_filter = {
      "type": "RangeFilter",
      "field_name": "cloud_cover",
      "config": {
        "lte": lte_clouds
      }
    }

    # combine our geo, date, cloud filters
    combined_filter = {
      "type": "AndFilter",
      "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }
    
    return combined_filter


# In[29]:


for i in range(0,8):
    geojson_geometry = get_bounding_box(CAMS[i], size)
    for month in range(1,13):
        combined_filter = create_filter(geojson_geometry, year, month, cloud_cover_lte)


# In[89]:


download_items = []

for i in range(0,1):
    geojson_geometry = get_bounding_box(CAMS[i], size)
    for month in range(1,13):
        year = 2018
        cloud_cover_lte = .01
        combined_filter = create_filter(geojson_geometry, year, month, cloud_cover_lte)
        print('CAMS-' + str(CAMS[i][0]) + ", " + str(month) + "-" + str(year))
        # API request object
        search_request = {
          "interval": "day",
          "item_types": [item_type], 
          "filter": combined_filter
        }
        # fire off the POST request
        search_result =           requests.post(
            'https://api.planet.com/data/v1/quick-search',
            auth=HTTPBasicAuth(PLANET_API_KEY, ''),
            json=search_request)
        
        # extract image IDs
        image_ids = [feature['id'] for feature in search_result.json()['features']]
        # extract the origin_x and origin_y
        origin_x = [feature['properties']['origin_x'] for feature in search_result.json()['features']]
        origin_y = [feature['properties']['origin_y'] for feature in search_result.json()['features']]
        epsg_code = [feature['properties']['epsg_code'] for feature in search_result.json()['features']]
        
        # Use the version 2 of orders API to clip images to the area of interest after item id acquisition.
        orders_url = 'https://api.planet.com/compute/ops/orders/v2'

        # set up requests to work with api
        auth = HTTPBasicAuth(PLANET_API_KEY, '')
        headers = {'content-type': 'application/json'}

        # define products part of order
        same_src_products = [
            {
              "item_ids": image_ids,
              "item_type": "PSScene4Band",
              "product_bundle": "analytic_sr"
            }
        ]

        for x in range(0,len(same_src_products[i]['item_ids'])):
            if len(same_src_products[i]) > 0:
                print(same_src_products[i]['item_ids'][x])
                # For demo purposes, just grab the first image ID
                id0 = same_src_products[i]['item_ids'][x]
                id0_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, id0)

                # Returns JSON metadata for assets in this ID. Learn more: planet.com/docs/reference/data-api/items-assets/#asset
                result =                   requests.get(
                    id0_url,
                    auth=HTTPBasicAuth(PLANET_API_KEY, '')
                  )
                
                # List of asset types available for this particular satellite image
                if 'analytic_sr' in list(result.json().keys()):
                    download_items.append(id0)


# In[ ]:


# How many elements each list should have
n = 100
# Splits list into chunks of the maximum download capability (100).
final = [download_items[i * n:(i + 1) * n] for i in range((len(download_items) + n - 1) // n )]

for i in range(0,len(final)):
    # Update order items to only those with analytic_sr
    same_src_products = [
        {
          "item_ids": final[i],
          "item_type": "PSScene4Band",
          "product_bundle": "analytic_sr"
        }
    ]
    # Get the clipping area.
    clip_aoi = get_bounding_box(CAMS[0], size)

    # define the clip tool
    clip = {
        "clip": {
            "aoi": clip_aoi
        }
    }

    # create an order request with the clipping tool
    request_clip = {
      "name": "just clip",
      "products": same_src_products,
      "tools": [clip]
    }

    # allow for caching so we don't always run clip
    run_clip = True

    clip_img_file = '9f6eb896-5405-4f00-894d-f366e585d9a5/1/files/20180122_163308_0f18_AnalyticMS_SR_clip.tif'
    if os.path.isfile(clip_img_file): 
        run_clip = False
    
    if run_clip:
        clip_order_url = place_order(request_clip, auth)
        poll_for_success(clip_order_url, auth)
        downloaded_clip_files = download_order(clip_order_url, auth)
        clip_img_file = next(downloaded_clip_files[d] for d in downloaded_clip_files
                         if d.endswith('_3B_AnalyticMS_SR_clip.tif'))
        clip_img_file


# In[ ]:


# For demo purposes, just grab the first image ID
id0 = '20180122_163308_0f18'
id0_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, id0)

# Returns JSON metadata for assets in this ID. Learn more: planet.com/docs/reference/data-api/items-assets/#asset
result =   requests.get(
    id0_url,
    auth=HTTPBasicAuth(PLANET_API_KEY, '')
  )

# List of asset types available for this particular satellite image
print(result.json().keys())


# In[ ]:


# Use the version 2 of orders API to clip images to the area of interest after item id acquisition.
orders_url = 'https://api.planet.com/compute/ops/orders/v2'

# set up requests to work with api
auth = HTTPBasicAuth(PLANET_API_KEY, '')
headers = {'content-type': 'application/json'}

# define products part of order
same_src_products = [
    {
      "item_ids": imageid[1],
      "item_type": "PSScene4Band",
      "product_bundle": "analytic_sr"
    }
]


# In[94]:


# Get the clipping area.
clip_aoi = get_bounding_box(CAMS[0], size)

# define the clip tool
clip = {
    "clip": {
        "aoi": clip_aoi
    }
}

# create an order request with the clipping tool
request_clip = {
  "name": "just clip",
  "products": same_src_products,
  "tools": [clip]
}

# allow for caching so we don't always run clip
run_clip = True

clip_img_file = '9f6eb896-5405-4f00-894d-f366e585d9a5/1/files/20180122_163308_0f18_AnalyticMS_SR_clip.tif'
if os.path.isfile(clip_img_file): run_clip = False


# In[95]:


get_bounding_box(CAMS[0], size)


# In[96]:


if run_clip:
    clip_order_url = place_order(request_clip, auth)
    poll_for_success(clip_order_url, auth)
    downloaded_clip_files = download_order(clip_order_url, auth)
    clip_img_file = next(downloaded_clip_files[d] for d in downloaded_clip_files
                     if d.endswith('_3B_AnalyticMS_SR_clip.tif'))
clip_img_file


# In[ ]:


# Display the results.
show_rgb('data/84bd4757-c9a6-4481-b8e3-c479a4219d8f/1/files/20180122_163308_0f18_3B_AnalyticMS_SR_clip.tif')


# In[ ]:




