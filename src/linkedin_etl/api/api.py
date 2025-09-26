import time
from requests import get, HTTPError
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def get_request(url, headers, delay=1):
    if delay:
        time.sleep(1)
    response = get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def add_parameters(url: str, count: int = 50, start: int = 0):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    query["count"] = [str(count)]
    query["start"] = [str(start)]

    new_query = urlencode(query, doseq=True, safe=":,()")

    return urlunparse(parsed._replace(query=new_query))

def get_jobs(url: str, headers: dict):
        
        url = add_parameters(url)

        get_job_id = lambda job:   job.get("jobCardUnion",{}).\
                                    get("jobPostingCard",{}).\
                                    get("preDashNormalizedJobPostingUrn","").\
                                    split(":")[-1]
        
        start = 0
        step = 50

        while True:
            print(f"Fetching jobs, current pagination: {start}")
            job_response = get_request(url, headers=headers)
            if not job_response["elements"]:
                break

            total_jobs = job_response.get("paging",{}).\
                                        get("total",0)

            elements = job_response["elements"]
            
            yield {"total_jobs": int(total_jobs), "jobs": [int(get_job_id(element)) for element in elements if get_job_id(element)]}
            
            start += step
            url = add_parameters(url=url, start=start)

def get_job_details(url: str, headers: dict): 
    try:
        job_detail_response = get_request(url, headers)
    except HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            # It's possible (for whatever reason) that a job detail is not available
            # in this case we ignore it and continue the rest of the ETL pipeline
            return ({}, {})
        raise

    job_information = {}
    company_information = {}
    
    if job_detail_response:
        job_information["job_id"] =  job_detail_response.get("jobPostingId",-1)
        job_information["job_name"] = job_detail_response.get("title", "No Data Available")
        job_information["standardized_name"] = job_detail_response.get("standardizedTitleResolutionResult", {})\
            .get("localizedName","No Data Available")
        job_information["job_url"] = job_detail_response.get("jobPostingUrl", "No Data Available")
        job_information["job_description"] = __clean_string(job_detail_response.get("description", {})\
            .get("text","No Data Available"))
        job_information["job_type"] = job_detail_response.get("formattedEmploymentStatus", "No Data Available")
        
        salary_insights = job_detail_response.get("salaryInsights", {}).\
                                                get("compensationBreakdown",[])
        
        if salary_insights:
            job_information["job_min_salary"] = salary_insights[0].get("minSalary", 0)
            job_information["job_max_salary"] = salary_insights[0].get("maxSalary", 0)
            job_information["job_pay_period"] = salary_insights[0].get("payPeriod", "No Data Available")

        job_information["job_functions"] = '|'.join(job_detail_response.get("formattedJobFunctions",[]))
        job_information["job_experience_level"] = job_detail_response.get("formattedExperienceLevel","No Data Available")
        job_information["job_views"] = job_detail_response.get("views",-1)
        
        company_details = job_detail_response.get("companyDetails", {}).\
            get("com.linkedin.voyager.deco.jobs.web.shared.WebJobPostingCompany",{}).\
            get("companyResolutionResult",{})

        if company_details:
            company_information = __get_company_details(company_details)
            job_information["company_id"] = company_information.get("company_id")

    return (job_information, company_information)

def __get_company_details(company_data):
    company_details = {}

    company_details["company_id"] = int(company_data.get("entityUrn","").split(":")[-1])
    company_details["company_name"] = company_data.get("universalName","No Data Available")
    
    company_details["company_image_url"] = "No Data Available"

    company_logo = company_data.get("logo",{}).\
                                    get("image",{}).\
                                    get("com.linkedin.common.VectorImage",{})
    
    if company_logo:
        image_data = company_logo.get("artifacts",[{}])[0].\
                            get("fileIdentifyingUrlPathSegment","")
        root_url = company_logo.get("rootUrl","")

        if image_data and root_url:
            company_details["company_image_url"] = root_url+image_data   
    
    
    company_details["company_description"] = __clean_string(company_data.get("description","No Data Available"))
    company_details["company_staff_count"] = company_data.get("staffCount",0)
    company_details["company_url"] = company_data.get("url",0)
    company_details["company_follower_count"] = company_data.get("followingInfo",{}).get("followerCount",0 )
    company_details["company_industries"] = '|'.join(company_data.get("industries",[]))

    return company_details

def __clean_string(input_string):
    return input_string.encode('utf-8', 'ignore').decode('utf-8')
