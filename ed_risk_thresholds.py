from pymongo import MongoClient
import urllib.parse
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from scipy.stats import poisson
from scipy.stats import lognorm
from scipy.stats import nbinom
import json
from bson import ObjectId

# Custom JSON encoder to convert ObjectId to string
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to a string
        return super(JSONEncoder, self).default(obj)
    
# MongoDB connection setup
def connect_to_mongo():
    host = "localhost:27017"
    username = "ed_admin_db"
    password = urllib.parse.quote("2D5fg7tg$%9jj")
    auth_source = "edengue"
    mongo_uri = f"mongodb://{username}:{password}@{host}/"
    db_name = 'edengue'
    collection_name = 'l2_model_input_2022'

    # Create a MongoDB client with authentication
    client = MongoClient(mongo_uri, authSource=auth_source)

    # Access the database and collection
    db = client[db_name]
    collection = db[collection_name]    

    return client, collection

# Function to extract total cases from a single feature (document)
def extract_total_cases_from_feature(doc, year_start=2004, year_end=2023):
    total_cases_data = {}
    # Check if 'total_cases' exists in the document
    total_cases = doc.get("properties", {}).get("total_cases", {})

    # Iterate over the years (2023, 2024, etc.)

    for i in range(year_start, year_end + 1):
        year = str(i)
        if year in total_cases:
            total_cases_data[year] = total_cases[year]  # Add cases for the current year
        else:
            total_cases_data[year] = [[1, 0, 0] for _ in range(12)] # Add 0 cases for the current year
            print(f"Year {year} data not found in {doc.get("properties", {}).get("fcode")}")
            
    return total_cases_data

# Function to model and plot total cases distribution for a single feature (document)
def model_total_cases_normal_for_feature(doc):
    total_cases_data = extract_total_cases_from_feature(doc)
    
    # Initialize arrays to collect all monthly data
    months_data = {month: [] for month in range(12)}  # Collect data for each month
    
    # Organize data by month across all years
    for year, cases in total_cases_data.items():
        for month, month_data in enumerate(cases):
            total_cases = month_data[0]  # First element is total cases
            months_data[month].append(total_cases)
    
    # Plot distribution and fit Gaussian for each month
    for month, data in months_data.items():
        data = [x for x in data if x > 0]
        plt.figure()
        plt.hist(data, bins=10, density=True, alpha=0.6, color='g')

        # Fit a Gaussian distribution
        mu, std = norm.fit(data)
        
        # Plot the Gaussian curve
        xmin, xmax = plt.xlim()
        x = np.linspace(xmin, xmax, 100)
        p = norm.pdf(x, mu, std)
        plt.plot(x, p, 'k', linewidth=2)
        
        plt.title(f"Feature {doc.get('_id')} - Month {month + 1} Cases Distribution\nMean = {mu:.2f}, Std Dev = {std:.2f}")
        plt.xlabel("Total Cases")
        plt.ylabel("Frequency")
        plt.show()

# Function to model and plot total cases distribution for a single feature (document) using Poisson distribution
def model_total_cases_poisson_for_feature(doc):
    total_cases_data = extract_total_cases_from_feature(doc)
    
    # Initialize arrays to collect all monthly data
    months_data = {month: [] for month in range(12)}  # Collect data for each month

    # Organize data by month across all years
    for year, cases in total_cases_data.items():
        for month, month_data in enumerate(cases):
            total_cases = month_data[0]  # First element is total cases
            months_data[month].append(total_cases)
    
    # Plot distribution and fit Poisson for each month
    for month, data in months_data.items():
        if len(data) > 0:  # Only proceed if data is available for the month
            data = [x for x in data if x > 0]
            plt.figure()
            plt.hist(data, bins=10, density=True, alpha=0.6, color='g')

            # Fit a Poisson distribution (mean of the data)
            mu = np.mean(data)  # Lambda (rate parameter) is the mean of the data

            # Generate Poisson probability mass function (PMF) for plotting
            x = np.arange(0, max(data) + 1)
            pmf = poisson.pmf(x, mu)
            
            plt.plot(x, pmf, 'k', marker='o', linestyle='--', label=f'Poisson (λ={mu:.2f})')
            
            plt.title(f"Feature {doc.get('_id')} - Month {month + 1} Cases Distribution\nPoisson λ = {mu:.2f}")
            plt.xlabel("Total Cases")
            plt.ylabel("Probability")
            plt.legend()
            plt.show()

# Function to model and plot total cases distribution for a single feature (document) using Log-Normal distribution
def model_total_cases_lognorm_for_feature(doc):
    total_cases_data = extract_total_cases_from_feature(doc)
    
    # Initialize arrays to collect all monthly data
    months_data = {month: [] for month in range(12)}  # Collect data for each month

    # Organize data by month across all years
    for year, cases in total_cases_data.items():
        for month, month_data in enumerate(cases):
            total_cases = month_data[0]  # First element is total cases
            months_data[month].append(total_cases)
    
    # Plot distribution and fit Log-Normal for each month
    for month, data in months_data.items():
        if len(data) > 0:  # Only proceed if data is available for the month
            data = [x for x in data if x > 0]
            plt.figure()
            plt.hist(data, bins=10, density=True, alpha=0.6, color='g')

            # Fit a Log-Normal distribution
            shape, loc, scale = lognorm.fit(data, floc=0)  # fit log-normal parameters
            
            # Generate Log-Normal probability density function (PDF) for plotting
            x = np.linspace(min(data), max(data), 100)
            pdf = lognorm.pdf(x, shape, loc, scale)
            
            plt.plot(x, pdf, 'k', linewidth=2, label=f'Log-Normal\nShape = {shape:.2f}, Scale = {scale:.2f}')
            
            plt.title(f"Feature {doc.get('_id')} - Month {month + 1} Cases Distribution\nLog-Normal Fit")
            plt.xlabel("Total Cases")
            plt.ylabel("Density")
            plt.legend()
            plt.show()


# # Function to model total cases distribution for a single feature (document) using Poisson and Log-Normal distributions
# def create_monthly_distribution_models_for_feature(doc, year_start, year_end):
#     # Get the historical cases data for modelling
#     historical_cases_data = extract_total_cases_from_feature(doc, year_start, year_end)    

#     # Initialize arrays to collect all monthly data
#     months_data = {month: [] for month in range(12)}  # Collect data for each month

#     model_parameters = {
#         #"fcode": doc.get("properties", {}).get("fcode"),
#         "poisson_models": {},
#         "lognorm_models": {},
#     }

#     # Organize data by month across all years
#     for year, cases in historical_cases_data.items():
#         for month, month_data in enumerate(cases):
#             total_cases = month_data[0]  # First element is total cases
#             months_data[month].append(total_cases)
    
#     # Fit models for each month data
#     for month, dataitem in months_data.items():
#         data = [x for x in dataitem if x > 0]
#         if len(data) > 0:  # Only proceed if data is available for the month            
#             poisson_lambda = np.mean(data)
#             low, medium = find_poisson_risk_thresholds(poisson_lambda)
#             model_parameters["poisson_models"][f"month_{month+1}"] = {
#                 "lamda": poisson_lambda,
#                 "risk_threshold_low": int(low),
#                 "risk_threshold_medium": int(medium)
#             }

#             # Fit a Log-Normal distribution
#             shape, loc, scale = lognorm.fit(data, floc=0)  # fit log-normal parameters
#             low, medium = find_lognorm_risk_thresholds(shape, loc, scale)

#             # Calculate mean and standard deviation of the log-normal distribution
#             mu_log = np.log(scale)  # scale is exp(mu_log), so we take log of scale
#             sigma_log = shape       # shape corresponds to sigma_log
            
#             # Mean of the log-normal distribution
#             mean = np.exp(mu_log + (sigma_log**2) / 2)
            
#             # Standard deviation of the log-normal distribution
#             std_dev = np.exp(mu_log + (sigma_log**2) / 2) * np.sqrt(np.exp(sigma_log**2) - 1) 

#             model_parameters["lognorm_models"][f"month_{month+1}"] = {
#                 "shape": shape,
#                 "loc": loc,
#                 "scale": scale,
#                 "mean" : mean,
#                 "sd": std_dev,
#                 "risk_threshold_low": int(low),
#                 "risk_threshold_medium": int(medium)
#             }
#         else:
#             print(month,":",dataitem)

#     # Get the current cases data for checking
#     current_total_cases = doc.get("properties", {}).get("total_cases", {}).get(str(year_end))
#     if current_total_cases:
#         if len(current_total_cases) == 12:
#             for month in range(12):
#                 total_monthly_cases = current_total_cases[month][0]

#                 # Poisson risk
#                 if total_monthly_cases <= model_parameters["poisson_models"][f"month_{month+1}"].get("risk_threshold_low"):
#                     model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "LOW"                    
#                 elif total_monthly_cases <= model_parameters["poisson_models"][f"month_{month+1}"].get("risk_threshold_medium"):
#                     model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
#                 else:
#                     model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
#                 model_parameters["poisson_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases

#                 # Log-normal risk
#                 if total_monthly_cases <= model_parameters["lognorm_models"][f"month_{month+1}"].get("risk_threshold_low"):
#                     model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "LOW"
#                 elif total_monthly_cases <= model_parameters["lognorm_models"][f"month_{month+1}"].get("risk_threshold_medium"):
#                     model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
#                 else:
#                     model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
#                 model_parameters["lognorm_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases
#     else:
#         print ("MISSING: ", doc.get("properties", {}).get("fcode"))
#     return model_parameters        

# Function to model total cases distribution for a single feature (document) using Poisson, Log-Normal, Negative Binomial, and Empirical distributions
def create_monthly_distribution_models_for_feature(doc, year_start, year_end):
    # Get the historical cases data for modelling
    historical_cases_data = extract_total_cases_from_feature(doc, year_start, year_end)    

    # Initialize arrays to collect all monthly data
    months_data = {month: [] for month in range(12)}  # Collect data for each month

    model_parameters = {
        #"fcode": doc.get("properties", {}).get("fcode"),
        "poisson_models": {},
        "lognorm_models": {},
        "neg_binom_models": {},
        "empirical_models": {},
    }

    # Organize data by month across all years
    for year, cases in historical_cases_data.items():
        for month, month_data in enumerate(cases):
            total_cases = month_data[0]  # First element is total cases
            months_data[month].append(total_cases)
    
    # Fit models for each month data
    for month, dataitem in months_data.items():
        data = [x for x in dataitem if x > 0]
        if len(data) > 0:  # Only proceed if data is available for the month            
            # Poisson Distribution
            poisson_lambda = np.mean(data)
            low, medium = find_poisson_risk_thresholds(poisson_lambda)
            model_parameters["poisson_models"][f"month_{month+1}"] = {
                "lamda": poisson_lambda,
                "risk_threshold_low": int(low),
                "risk_threshold_medium": int(medium)
            }

            # Log-Normal Distribution
            shape, loc, scale = lognorm.fit(data, floc=0)  # fit log-normal parameters
            low, medium = find_lognorm_risk_thresholds(shape, loc, scale)

            mu_log = np.log(scale)  # scale is exp(mu_log), so we take log of scale
            sigma_log = shape  # shape corresponds to sigma_log

            mean = np.exp(mu_log + (sigma_log**2) / 2)
            std_dev = np.exp(mu_log + (sigma_log**2) / 2) * np.sqrt(np.exp(sigma_log**2) - 1)

            model_parameters["lognorm_models"][f"month_{month+1}"] = {
                "shape": shape,
                "loc": loc,
                "scale": scale,
                "mean": mean,
                "sd": std_dev,
                "risk_threshold_low": int(low),
                "risk_threshold_medium": int(medium)
            }

            # Negative Binomial Distribution
            mean_nb = np.mean(data)
            var_nb = np.var(data)
            if var_nb > mean_nb:
                p_nb = mean_nb / var_nb  # probability of success
                r_nb = mean_nb * p_nb / (1 - p_nb)  # number of failures
                low, medium = find_negative_binom_risk_thresholds(r_nb, p_nb)

                model_parameters["neg_binom_models"][f"month_{month+1}"] = {
                    "r": r_nb,
                    "p": p_nb,
                    "risk_threshold_low": int(low),
                    "risk_threshold_medium": int(medium)
                }

            # Empirical Distribution (non-parametric)
            low, medium = find_empirical_risk_thresholds(data)
            model_parameters["empirical_models"][f"month_{month+1}"] = {
                "data": data,
                "risk_threshold_low": int(low),
                "risk_threshold_medium": int(medium)
            }
        else:
            print(month, ":", dataitem)

    # Get the current cases data for checking
    current_total_cases = doc.get("properties", {}).get("total_cases", {}).get(str(year_end + 1))
    if current_total_cases:
        if len(current_total_cases) == 12:
            for month in range(12):
                total_monthly_cases = current_total_cases[month][0]

                # Poisson risk
                if total_monthly_cases <= model_parameters["poisson_models"][f"month_{month+1}"].get("risk_threshold_low"):
                    model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "LOW"                    
                elif total_monthly_cases <= model_parameters["poisson_models"][f"month_{month+1}"].get("risk_threshold_medium"):
                    model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
                else:
                    model_parameters["poisson_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
                model_parameters["poisson_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases

                # Log-normal risk
                if total_monthly_cases <= model_parameters["lognorm_models"][f"month_{month+1}"].get("risk_threshold_low"):
                    model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "LOW"
                elif total_monthly_cases <= model_parameters["lognorm_models"][f"month_{month+1}"].get("risk_threshold_medium"):
                    model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
                else:
                    model_parameters["lognorm_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
                model_parameters["lognorm_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases

                # Negative Binomial risk
                if "neg_binom_models" in model_parameters and f"month_{month+1}" in model_parameters["neg_binom_models"]:
                    if total_monthly_cases <= model_parameters["neg_binom_models"][f"month_{month+1}"].get("risk_threshold_low"):
                        model_parameters["neg_binom_models"][f"month_{month+1}"]["risk_level"] = "LOW"
                    elif total_monthly_cases <= model_parameters["neg_binom_models"][f"month_{month+1}"].get("risk_threshold_medium"):
                        model_parameters["neg_binom_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
                    else:
                        model_parameters["neg_binom_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
                    model_parameters["neg_binom_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases

                # Empirical risk
                if "empirical_models" in model_parameters and f"month_{month+1}" in model_parameters["empirical_models"]:
                    if total_monthly_cases <= model_parameters["empirical_models"][f"month_{month+1}"].get("risk_threshold_low"):
                        model_parameters["empirical_models"][f"month_{month+1}"]["risk_level"] = "LOW"
                    elif total_monthly_cases <= model_parameters["empirical_models"][f"month_{month+1}"].get("risk_threshold_medium"):
                        model_parameters["empirical_models"][f"month_{month+1}"]["risk_level"] = "MEDIUM"
                    else:
                        model_parameters["empirical_models"][f"month_{month+1}"]["risk_level"] = "HIGH"
                    model_parameters["empirical_models"][f"month_{month+1}"]["total_monthly_cases"] = total_monthly_cases
    else:
        print("MISSING: ", doc.get("properties", {}).get("fcode"))
    return model_parameters

# Function to save all model parameters to a single JSON file
# Function to save all model parameters as a GeoJSON FeatureCollection
def save_all_model_parameters_to_json(all_model_parameters, year):
    # Wrap features in a FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": all_model_parameters
    }
    
    # Define the filename based on the year
    json_filename = f"all_features_models_{year}.json"
    
    # Save the FeatureCollection to a JSON file
    with open(json_filename, 'w', encoding='utf-8') as json_file:
        json.dump(feature_collection, json_file, cls=JSONEncoder, ensure_ascii=False, indent=4)
    
    print(f"All model parameters saved to {json_filename}")

# Function to find the risk thresholds for Poisson distribution
def find_poisson_risk_thresholds(lambda_poisson):
    # Parameters
    I0 = 1  # Baseline impact
    alpha = 0.01  # Growth rate
    population = 1e5  # Population
    n_max = poisson.ppf(0.999, lambda_poisson)  # Find Nmax such that P(Nmax) ~ 0 (99.9% of the mass)
    n_values = np.arange(0, int(n_max) + 1)  # Values of n from 0 to Nmax

    # Function to calculate impact
    def impact(n, I0, alpha, population):
        return np.exp(n/lambda_poisson)
        #return I0 * n * (population / 1e5) * np.exp(alpha * n * (population / 1e5))

    # Calculate risk for each n
    probabilities = poisson.pmf(n_values, lambda_poisson)
    impacts = impact(n_values, I0, alpha, population)
    risks = probabilities * impacts

    # Total risk sum
    total_risk = np.sum(risks)

    # Find thresholds that divide the risk space into three equal areas
    risk_cumsum = np.cumsum(risks)
    low_threshold_index = np.searchsorted(risk_cumsum, total_risk / 3)
    medium_threshold_index = np.searchsorted(risk_cumsum, 2 * total_risk / 3)

    # Extract thresholds
    low_threshold = n_values[low_threshold_index]
    medium_threshold = n_values[medium_threshold_index]

    return low_threshold, medium_threshold

# Function to find the risk thresholds for Log-normal distribution
def find_lognorm_risk_thresholds(shape, loc, scale):
    # Parameters
    I0 = 1  # Baseline impact
    alpha = 0.02  # Growth rate
    population = 1e5  # Population

    n_max = int(lognorm.ppf(0.999, s=shape, loc=loc, scale=scale))  # Find Nmax such that P(Nmax) ~ 0    
    n_values = np.arange(0, int(n_max) + 1)  # Values of n from 0 to Nmax

    # Function to calculate impact
    def impact(n, I0, alpha, population):        

        # Calculate mean and standard deviation of the log-normal distribution
        mu_log = np.log(scale)  # scale is exp(mu_log), so we take log of scale
        sigma_log = shape       # shape corresponds to sigma_log
        
        # Mean of the log-normal distribution
        mean = np.exp(mu_log + (sigma_log**2) / 2)

        return np.exp(alpha * n / mean)
        #return np.exp((n-mean)/mean)
        #return I0 * n * (population / 1e5) * np.exp(alpha * n * (population / 1e5))

    # Calculate risk for each n
    probabilities = lognorm.pdf(n_values, s=shape, loc=loc, scale=scale)
    impacts = impact(n_values, I0, alpha, population)
    risks = probabilities * impacts

    # Total risk sum
    total_risk = np.sum(risks)

    # Find thresholds that divide the risk space into three equal areas
    risk_cumsum = np.cumsum(risks)
    low_threshold_index = np.searchsorted(risk_cumsum, total_risk / 3)
    medium_threshold_index = np.searchsorted(risk_cumsum, 2 * total_risk / 3)

    # Extract thresholds
    low_threshold = n_values[low_threshold_index]
    medium_threshold = n_values[medium_threshold_index]

    return low_threshold, medium_threshold

# Function to find the risk thresholds for Negative Binomial distribution
def find_negative_binom_risk_thresholds(r, p):
    # Parameters
    I0 = 1  # Baseline impact
    alpha = 0.02  # Growth rate
    population = 1e5  # Population

    # Calculate n_max where P(Nmax) ~ 0 for Negative Binomial
    n_max = int(nbinom.ppf(0.999, r, p))  # Find Nmax for cumulative probability ~ 0.999
    n_values = np.arange(0, n_max + 1)  # Values of n from 0 to Nmax

    # Function to calculate impact based on growth
    def impact(n, I0, alpha, population):
        mean = r * (1 - p) / p  # Mean of the Negative Binomial distribution
        return np.exp(alpha * n / mean)

    # Calculate probability and impact for each n
    probabilities = nbinom.pmf(n_values, r, p)
    impacts = impact(n_values, I0, alpha, population)
    risks = probabilities * impacts

    # Total risk sum
    total_risk = np.sum(risks)

    # Find thresholds that divide the risk space into three equal parts
    risk_cumsum = np.cumsum(risks)
    low_threshold_index = np.searchsorted(risk_cumsum, total_risk / 3)
    medium_threshold_index = np.searchsorted(risk_cumsum, 2 * total_risk / 3)

    # Extract thresholds
    low_threshold = n_values[low_threshold_index]
    medium_threshold = n_values[medium_threshold_index]

    return low_threshold, medium_threshold

# Function to find the risk thresholds for Empirical distribution
def find_empirical_risk_thresholds(data):
    # Parameters
    I0 = 1  # Baseline impact
    alpha = 0.002  # Growth rate
    population = 1e5  # Population

    # Sort the empirical data to create the empirical CDF
    data = sorted(data)
    n_values = np.array(data)

    # Function to calculate impact based on growth
    def impact(n, I0, alpha, population):
        return I0 * n * (population / 1e5) * np.exp(alpha * n * (population / 1e5))
        mean = np.mean(n_values)  # Empirical mean
        return np.exp(alpha * n / mean)

    # Calculate probability for each n (assume uniform empirical distribution)
    probabilities = np.ones(len(n_values)) / len(n_values)  # Uniform probability for empirical data
    impacts = impact(n_values, I0, alpha, population)
    risks = probabilities * impacts

    # Total risk sum
    total_risk = np.sum(risks)

    # Find thresholds that divide the risk space into three equal parts
    risk_cumsum = np.cumsum(risks)
    low_threshold_index = np.searchsorted(risk_cumsum, total_risk / 3)
    medium_threshold_index = np.searchsorted(risk_cumsum, 2 * total_risk / 3)

    # Extract thresholds
    low_threshold = n_values[low_threshold_index]
    medium_threshold = n_values[medium_threshold_index]

    return low_threshold, medium_threshold

# Function to identify features with missing total_cases data between 2004-2022
def find_features_with_missing_total_cases(collection, year_start, year_end):
    missing_data_features = []

    # Iterate over each document (feature) in the collection
    for doc in collection.find():
        missing_info = {
            "_id": doc.get("_id"),
            "fcode": doc.get("properties", {}).get("fcode"),
            "missing_years": [],
            "missing_months": {}
        }
        
        # Get the total_cases data
        total_cases = doc.get("properties", {}).get("total_cases", {})
        
        # Check for missing years between 2004 and 2022
        for year in range(year_start, year_end + 1):
            year_str = str(year)
            if year_str not in total_cases:
                missing_info["missing_years"].append(year_str)
            else:
                # Check for missing months (ensure there are 12 months of data)
                if len(total_cases[year_str]) != 12:
                    missing_months = [month+1 for month in range(12) if month >= len(total_cases[year_str])]
                    missing_info["missing_months"][year_str] = missing_months

        # If any years or months are missing, add this feature to the result
        if missing_info["missing_years"] or missing_info["missing_months"]:
            missing_data_features.append((doc, missing_info))

    return missing_data_features

# Main execution
if __name__ == "__main__":
    # Connect to MongoDB
    client, collection = connect_to_mongo()
    all_model_parameters = []
    year_start = 2008 
    year_end = 2017

    for doc in collection.find():
        model_parameters = create_monthly_distribution_models_for_feature(doc, year_start, year_end)

        # Keep only 'geometry' and 'properties.address' fields, remove other properties except 'risk'
        doc_cleaned = { 
            "type": "Feature",           
            "properties": {
                "fcode": doc.get("properties", {}).get("fcode"),
                "address": doc["properties"].get("address"),
                "risk": model_parameters  # Add the model parameters under 'risk'
            },
            "geometry": doc.get("geometry")
        }

        all_model_parameters.append(doc_cleaned)        

    save_all_model_parameters_to_json(all_model_parameters, year_end)

    # # Find features with missing total_cases data
    # missing_features = find_features_with_missing_total_cases(collection)
    
    # # Print out the features with missing data
    # if missing_features:
    #     print("Features with missing total_cases data between 2004-2022:")
    #     for feature in missing_features:
    #         print(f"Feature ID: {feature['fcode']}")
    #         if feature["missing_years"]:
    #             print(f"  Missing Years: {', '.join(feature['missing_years'])}")
    #         if feature["missing_months"]:
    #             print("  Missing Months:")
    #             for year, months in feature["missing_months"].items():
    #                 print(f"    Year {year}: Missing Months {months}")
    # else:
    #     print("No missing total_cases data found between 2004-2022.")

    # Process each feature (document) individually
    # for doc in collection.find():
    #     print(f"Modeling total cases for feature {doc.get("properties", {}).get("fcode")}")
    #     #model_total_cases_distribution_for_feature(doc)
    #     #model_total_cases_poisson_for_feature(doc)
    #     model_total_cases_lognorm_for_feature(doc)
    #     break

    client.close()

