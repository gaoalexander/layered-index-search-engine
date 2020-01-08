import json

def formatAllRecipes(filePath, jsonData):
    with open(filePath, encoding="utf8") as inputFile:
        for line in inputFile:
            jsonValue = json.loads(line)
            crawledFormatJson = {}
            if "title" in jsonValue and "url" in jsonValue:
                crawledFormatJson["title"] = jsonValue["title"]
                crawledFormatJson["url"] = jsonValue["url"]
                crawledFormatJson["rating"] = jsonValue["rating_stars"] if "rating_stars" in jsonValue else 3.4
                crawledFormatJson["ingredients"] = jsonValue["ingredients"] if "ingredients" in jsonValue else [
                ]
                crawledFormatJson["reviews_count"] = jsonValue["review_count"] if "review_count" in jsonValue else 0
                crawledFormatJson["make_again_percentage"] = jsonValue["willMakeAgainPct"] if "willMakeAgainPct" in jsonValue else 0
                crawledFormatJson["cook_time_minutes"] = jsonValue["cooking_time_minutes"] if "cooking_time_minutes" in jsonValue else 120
                crawledFormatJson["instructions"] = jsonValue["instructions"] if "instructions" in jsonValue else [
                ]
                jsonData.append(crawledFormatJson)
        return jsonData

def formatBbcCoukRecipes(filePath, jsonData):
    with open(filePath, encoding="utf8") as inputFile:
        for line in inputFile:
            jsonValue = json.loads(line)
            crawledFormatJson = {}
            if "title" in jsonValue and "url" in jsonValue:
                crawledFormatJson["title"] = jsonValue["title"]
                crawledFormatJson["url"] = jsonValue["url"]
                crawledFormatJson["rating"] = jsonValue["rating_count"] if "rating_count" in jsonValue else 3.4
                crawledFormatJson["ingredients"] = jsonValue["ingredients"] if "ingredients" in jsonValue else [
                ]
                crawledFormatJson["reviews_count"] = jsonValue["reviewsCount"] if "reviewsCount" in jsonValue else 0
                crawledFormatJson["make_again_percentage"] = jsonValue["willMakeAgainPct"] if "willMakeAgainPct" in jsonValue else 0
                crawledFormatJson["cook_time_minutes"] = jsonValue["cooking_time_minutes"] if "cooking_time_minutes" in jsonValue else 120
                crawledFormatJson["instructions"] = jsonValue["instructions"] if "instructions" in jsonValue else [
                ]
                jsonData.append(crawledFormatJson)
        return jsonData

def formatCookStrRecipes(filePath, jsonData):
    with open(filePath, encoding="utf8") as inputFile:
        for line in inputFile:
            jsonValue = json.loads(line)
            crawledFormatJson = {}
            if "title" in jsonValue and "url" in jsonValue:
                crawledFormatJson["title"] = jsonValue["title"]
                crawledFormatJson["url"] = jsonValue["url"]
                crawledFormatJson["rating"] = jsonValue["rating_count"] if "rating_count" in jsonValue else 3.4
                crawledFormatJson["ingredients"] = jsonValue["ingredients"] if "ingredients" in jsonValue else [
                ]
                crawledFormatJson["reviews_count"] = jsonValue["reviewsCount"] if "reviewsCount" in jsonValue else 0
                crawledFormatJson["make_again_percentage"] = jsonValue["willMakeAgainPct"] if "willMakeAgainPct" in jsonValue else 0
                crawledFormatJson["cook_time_minutes"] = jsonValue["total_time"] if "total_time" in jsonValue else 120
                crawledFormatJson["instructions"] = jsonValue["instructions"] if "instructions" in jsonValue else [
                ]
                jsonData.append(crawledFormatJson)
        return jsonData


def formatEpiCuriousRecipes(filePath, jsonData):
    with open(filePath, encoding="utf8") as inputFile:
        for line in inputFile:
            jsonValue = json.loads(line)
            crawledFormatJson = {}
            if "hed" in jsonValue and "url" in jsonValue:
                crawledFormatJson["title"] = jsonValue["hed"]
                crawledFormatJson["url"] = "https://www.epicurious.com" + \
                    jsonValue["url"]
                crawledFormatJson["rating"] = jsonValue["aggregateRating"] if "aggregateRating" in jsonValue else 3.4
                crawledFormatJson["ingredients"] = jsonValue["ingredients"] if "ingredients" in jsonValue else [
                ]
                crawledFormatJson["reviews_count"] = jsonValue["reviewsCount"] if "reviewsCount" in jsonValue else 0
                crawledFormatJson["make_again_percentage"] = jsonValue["willMakeAgainPct"] if "willMakeAgainPct" in jsonValue else 0
                crawledFormatJson["cook_time_minutes"] = 120
                crawledFormatJson["instructions"] = jsonValue["prepSteps"] if "prepSteps" in jsonValue else [
                ]
                jsonData.append(crawledFormatJson)
        return jsonData


def formatCrawledData():
    jsonData = []
    with open('crawled_formatted_data.json', 'w') as outputFile:
        jsonData = formatEpiCuriousRecipes('./dataset/epicurious-recipes.json', jsonData)
        jsonData = formatCookStrRecipes('./dataset/cookstr-recipes.json', jsonData)
        jsonData = formatCookStrRecipes('./dataset/bbccouk-recipes.json', jsonData)
        jsonData = formatCookStrRecipes('./dataset/allrecipes-recipes.json', jsonData)
        json.dump(jsonData, outputFile, indent=4)


def main():
    formatCrawledData()


if __name__ == "__main__":
    main()
