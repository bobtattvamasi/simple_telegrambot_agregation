# utils.py

from datetime import datetime, timedelta



def find_values_by_date_aggregation(collection, dt_from, dt_upto):
  """
  Finds documents with 'dt' field matching the provided date using aggregation.

  Args:
      collection: A MongoDB collection object.
      dates: A ISO object representing the desired date.

  Returns:
      A list of dictionaries containing documents with matching 'dt' field.
  """

  date = {
    "dt": {
        "$gte": datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S"),
        "$lt": datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S")
    }
}

  pipeline = [
      {"$match": date}  # Filter by matching 'dt' field
  ]
  results = list(collection.aggregate(pipeline))
  return results



def format_data(data, group_type):
  """
  Formats data dictionary into desired output structure.

  Args:
      data (dict): Dictionary containing salary data with keys as strings.
      group_type: String indicating desired granularity ('hour', 'day', or 'month').

  Returns:
      dict: Dictionary with "dataset" and "labels" keys in desired format.
  """

  dataset = list(data.values())
  if group_type=='hour':
    #print(f"hours = {data.keys()}")
    # Create labels for hourly data (00:00 to 23:00)
    labels = [f"{hour}:00:00" for hour in data.keys()]
  elif group_type == 'month':
    # Create labels for daily data (YYYY-MM-DD format)
    #print(f"monthes = {data.keys()}")
    labels = [f"{month}-{'01'}T00:00:00" for month in data.keys()]
  elif group_type == 'day':
    #print(f"days = {data.keys()}")
    # Create labels for daily data (YYYY-MM-DD format)
    labels = [f"{day}T00:00:00" for day in data.keys()]

  return {"dataset": dataset, "labels": labels}

def calculate_salary(documents, group_type):
  """
  Calculates total salary for a given granularity (hour, day, month).

  Args:
      documents: A list of dictionaries representing documents from the collection.
      group_type: String indicating desired granularity ('hour', 'day', or 'month').

  Returns:
      A dictionary containing total salary for each unit within the granularity.
  """

  salary_data = {}
  for doc in documents:
    dt = doc['dt']
    value = doc['value']  # Assuming 'value' represents salary

    if group_type == 'hour':
      key = dt.strftime("%Y-%m-%dT%H")  # Group by hour (0 to 23)
    elif group_type == 'day':
      key = dt.strftime("%Y-%m-%d")  # Group by year-month-day
    elif group_type == 'month':
      key = dt.strftime("%Y-%m")  # Group by year-month

    salary_data[key] = salary_data.get(key, 0) + value  # Add salary to existing for the key

    formatted_data = format_data(salary_data, group_type)

  return formatted_data