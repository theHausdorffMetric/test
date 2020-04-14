"""
This module aims to provide a very simple interface to solve captcha in spiders.

Make sure that any validation step is specified and performed in your spider
module. For extendability and maintainability purposes, validation should be
spider's responsability since captchas are very diverse.
"""

import io

from PIL import Image
from pytesseract import image_to_string


def solve_captcha(response, **kwargs):
    """Solve captcha using Tesseract OCR.

    Args:
        response: Scrapy response object to GET request performed on captcha image

    Returns:
        str: computed solution to the provided captcha


    """
    img_bytes = io.BytesIO(response.body)  # get image bytes to be processed
    img = Image.open(img_bytes)  # open image for further processing
    return image_to_string(img, *kwargs)
