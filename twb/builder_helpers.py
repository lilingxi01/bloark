import re

category_pattern = re.compile(r'^\[\[Category:(.+?)\]\]($|\n)', flags=re.MULTILINE)


def extract_categories(text):
    categories = category_pattern.findall(text)
    return [category[0] for category in categories]
