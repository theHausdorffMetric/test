"""
This module contains the spider code to scrape data from the Peruvian Customs national website.

This data source is really valuable since it contains information about bills of ladding of
vessels' cargo such as: date of unloading, port destination, weight of the cargo, and information
about the product and its grade.

Technical specificities:
    - The website limits the amount of data you can query to 30 days
    - Relevant information is protected by a captcha
"""
