             # Convert the response to JSON
            
            #performance columns function
            
            new_contacts = data['response']['report']['metrics'][0]['value']
            new_contacts__target = data['response']['report']['metrics'][1]['value']
            new_contacts__target_percentage = data['response']['report']['metrics'][2]['value']
            call_attempts = data['response']['report']['metrics'][3]['value']
            call_attempts__target = data['response']['report']['metrics'][4]['value']
            call_attempts__target_percentage = data['response']['report']['metrics'][5]['value']
            call_connections = data['response']['report']['metrics'][6]['value']
            call_connections__target = data['response']['report']['metrics'][7]['value']
            call_connections__target_percentage = data['response']['report']['metrics'][8]['value']
            appraisals = data['response']['report']['metrics'][9]['value']
            appraisals__target = data['response']['report']['metrics'][10]['value']
            appraisals__target_percentage = data['response']['report']['metrics'][11]['value']
            appraisals__total_value = data['response']['report']['metrics'][12]['value']
            appraisals__average_value = data['response']['report']['metrics'][13]['value']
            appraisals_split = data['response']['report']['metrics'][14]['value']
            appraisals__total_value_split = data['response']['report']['metrics'][15]['value']
            appraisals__average_value_split = data['response']['report']['metrics'][16]['value']
            appraise_to_present_ratio = data['response']['report']['metrics'][17]['value']
            appraise_to_list_ratio = data['response']['report']['metrics'][18]['value']
            presentations = data['response']['report']['metrics'][19]['value']
            presentations__target = data['response']['report']['metrics'][20]['value']
            presentations__target_percentage = data['response']['report']['metrics'][21]['value']
            presentations__total_value = data['response']['report']['metrics'][22]['value']
            presentations__average_value = data['response']['report']['metrics'][23]['value']
            presentations_split = data['response']['report']['metrics'][24]['value']
            presentations__total_value_split = data['response']['report']['metrics'][25]['value']
            presentations__average_value_split = data['response']['report']['metrics'][26]['value']
            present_to_list_ratio = data['response']['report']['metrics'][27]['value']
            listings = data['response']['report']['metrics'][28]['value']
            listings_split = data['response']['report']['metrics'][29]['value']
            listings__target = data['response']['report']['metrics'][30]['value']
            listings__target_percentage = data['response']['report']['metrics'][31]['value']
            listings__total_value = data['response']['report']['metrics'][32]['value']
            listings__total_value_split = data['response']['report']['metrics'][33]['value']
            listings__average_value = data['response']['report']['metrics'][34]['value']
            listings__average_value_split = data['response']['report']['metrics'][35]['value']
            new_listings_on_market = data['response']['report']['metrics'][36]['value']
            list_to_sell_ratio = data['response']['report']['metrics'][37]['value']
            sales_written = data['response']['report']['metrics'][38]['value']
            sales_written_split = data['response']['report']['metrics'][39]['value']
            sales_written__target = data['response']['report']['metrics'][40]['value']
            sales_written__target_percentage = data['response']['report']['metrics'][41]['value']
            sales_written__total_value = data['response']['report']['metrics'][42]['value']
            sales_written__total_value_split = data['response']['report']['metrics'][43]['value']
            sales_written__average_value = data['response']['report']['metrics'][44]['value']
            sales_written__average_value_split = data['response']['report']['metrics'][45]['value']
            sales_written__gross_commission = data['response']['report']['metrics'][46]['value']
            sales_written__gross_commission_split = data['response']['report']['metrics'][47]['value']
            sales_written__gross_commission__target = data['response']['report']['metrics'][48]['value']
            sales_written__gross_commission__target_percentage = data['response']['report']['metrics'][49]['value']
            sales_written__average_gross_commission = data['response']['report']['metrics'][50]['value']
            sales_written__average_gross_commission_split = data['response']['report']['metrics'][51]['value']
            sales_written__average_gross_commission_rate_percentage = data['response']['report']['metrics'][52]['value']
            sales_settled = data['response']['report']['metrics'][53]['value']
            sales_settled_split = data['response']['report']['metrics'][54]['value']
            sales_settled__total_value = data['response']['report']['metrics'][55]['value']
            sales_settled__total_value_split = data['response']['report']['metrics'][56]['value']
            sales_settled__average_value = data['response']['report']['metrics'][57]['value']
            sales_settled__average_value_split = data['response']['report']['metrics'][58]['value']
            sales_settled__gross_commission = data['response']['report']['metrics'][59]['value']
            sales_settled__gross_commission_split = data['response']['report']['metrics'][60]['value']
            listings_lease = data['response']['report']['metrics'][61]['value']
            leased_listings = data['response']['report']['metrics'][62]['value']
            auction_clearance_rate_adjusted = data['response']['report']['metrics'][63]['value']
            auction_clearance_rate_auction_day = data['response']['report']['metrics'][64]['value']
            average_days_on_market_all_sold = data['response']['report']['metrics'][65]['value']
            average_days_on_market_auctions = data['response']['report']['metrics'][66]['value']
            average_days_on_market_pt = data['response']['report']['metrics'][67]['value']
            average_days_on_market_lease = data['response']['report']['metrics'][68]['value']

            df_dict = {
                    'AB_Instance': i['Office'],
                    'Office ID': i['id'],
                    'State': i['address_state'],
                    'Office Name': i['name'],
                    'Date': first_date,
                    'New Contacts': new_contacts,
                    'New Contacts - Target': new_contacts__target,
                    'New Contacts - Target %': new_contacts__target_percentage,
                    'Call Attempts': call_attempts,
                    'Call Attempts - Target': call_attempts__target,
                    'Call Attempts - Target %': call_attempts__target_percentage,
                    'Call Connections': call_connections,
                    'Call Connections - Target': call_connections__target,
                    'Call Connections - Target %': call_connections__target_percentage,
                    'Appraisals': appraisals,
                    'Appraisals - Target': appraisals__target,
                    'Appraisals - Target %': appraisals__target_percentage,
                    'Appraisals - Total Value': appraisals__total_value,
                    'Appraisals - Average Value': appraisals__average_value,
                    'Appraisals (Split)': appraisals_split,
                    'Appraisals - Total Value (Split)': appraisals__total_value_split,
                    'Appraisals - Average Value (Split)': appraisals__average_value_split,
                    'Appraise to Present Ratio': appraise_to_present_ratio,
                    'Appraise to List Ratio': appraise_to_list_ratio,
                    'Presentations': presentations,
                    'Presentations - Target': presentations__target,
                    'Presentations - Target %': presentations__target_percentage,
                    'Presentations - Total Value': presentations__total_value,
                    'Presentations - Average Value': presentations__average_value,
                    'Presentations (Split)': presentations_split,
                    'Presentations - Total Value (Split)': presentations__total_value_split,
                    'Presentations - Average Value (Split)': presentations__average_value_split,
                    'Present to List Ratio': present_to_list_ratio,
                    'Listings': listings,
                    'Listings (Split)': listings_split,
                    'Listings - Target': listings__target,
                    'Listings - Target %': listings__target_percentage,
                    'Listings - Total Value': listings__total_value,
                    'Listings - Total Value (Split)': listings__total_value_split,
                    'Listings - Average Value': listings__average_value,
                    'Listings - Average Value (Split)': listings__average_value_split,
                    'New Listings On Market': new_listings_on_market,
                    'List to Sell Ratio': list_to_sell_ratio,
                    'Sales Written': sales_written,
                    'Sales Written (Split)': sales_written_split,
                    'Sales Written - Target': sales_written__target,
                    'Sales Written - Target %': sales_written__target_percentage,
                    'Sales Written - Total Value': sales_written__total_value,
                    'Sales Written - Total Value (Split)': sales_written__total_value_split,
                    'Sales Written - Average Value': sales_written__average_value,
                    'Sales Written - Average Value (Split)': sales_written__average_value_split,
                    'Sales Written - Gross Commission': sales_written__gross_commission,
                    'Sales Written - Gross Commission (Split)': sales_written__gross_commission_split,
                    'Sales Written - Gross Commission - Target': sales_written__gross_commission__target,
                    'Sales Written - Gross Commission - Target %': sales_written__gross_commission__target_percentage,
                    'Sales Written - Average Gross Commission': sales_written__average_gross_commission,
                    'Sales Written - Average Gross Commission (Split)': sales_written__average_gross_commission_split,
                    'Sales Written - Average Gross Commission Rate %': sales_written__average_gross_commission_rate_percentage,
                    'Sales Settled': sales_settled,
                    'Sales Settled (Split)': sales_settled_split,
                    'Sales Settled - Total Value': sales_settled__total_value,
                    'Sales Settled - Total Value (Split)': sales_settled__total_value_split,
                    'Sales Settled - Average Value': sales_settled__average_value,
                    'Sales Settled - Average Value (Split)': sales_settled__average_value_split,
                    'Sales Settled - Gross Commission': sales_settled__gross_commission,
                    'Sales Settled - Gross Commission (Split)': sales_settled__gross_commission_split,
                    'Listings (Lease)': listings_lease,
                    'Leased Listings': leased_listings,
                    'Auction Clearance Rate (Adjusted)': auction_clearance_rate_adjusted,
                    'Auction Clearance Rate (Auction Day)': auction_clearance_rate_auction_day,
                    'Average Days on Market (All Sold)': average_days_on_market_all_sold,
                    'Average Days on Market (Auctions)': average_days_on_market_auctions,
                    'Average Days on Market (PT)': average_days_on_market_pt,
                    'Average Days on Market (Lease)': average_days_on_market_lease
                    }
                    
            df_dict2 = {
                'Date': first_date,
                'AB_Instance': i['Office'],
                'Office ID': i['id'],
                'State': i['address_state'],
                'Office Name': i['name'],
                'Total_Calls': call_connections,
                'Appraisals': appraisals,
                'Appraisals_Value': appraisals__total_value,
                'Listings': listings,
                'Listings_Value': listings__total_value,
                'Unconditional_Sales': sales_written,
                'Unconditional_Sales_Value': sales_written__total_value,
                'Unconditional_GCI': sales_written__gross_commission,
                'Settled_Sales': sales_settled,
                'Settled_Sales_Value': sales_settled__total_value,
                'Settled_GCI': sales_settled__gross_commission,
                        }
            df_list.append(df_dict)
            df_list2.append(df_dict2)
        except Exception as e:
                print(e)
                
    df = pd.DataFrame(df_list)
            
    path_folder = r"/home/lemuel-torrefiel/airflow/performance"
    filename1 = r"/all_performance_metrics_"
    dot_csv = '.csv'

    filename = path_folder + filename1 + x['realDate'] + dot_csv

    df.to_csv(filename, sep=',', index=False, encoding='utf-8')

    df2 = pd.DataFrame(df_list2)
            
    path_folder = r"/home/lemuel-torrefiel/airflow/performance"
    filename2 = r"/performance_output_"

    filename3 = path_folder + filename2 + x['realDate'] + dot_csv

    df2.to_csv(filename3, sep=',', index=False, encoding='utf-8')

    print(f'\nend of month --> {x['month']}')

    # start of sharepoint
    print(f'now uploading to sharepoint all_performance folder')

    # SharePoint site details
    sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL")
    username = os.getenv("SHAREPOINT_EMAIL")
    password = os.getenv("SHAREPOINT_PASSWORD")

    # SharePoint folder and local file details
    document_library_name = "Documents"
    folder_name = "Prod_CSVs"
    sub_folder = "all_performance_metrics"
    local_file_path = filename

    # Initialize the ClientContext
    ctx = ClientContext(sharepoint_site_url).with_user_credentials(username, password)

    # Get the folder
    folder = ctx.web.lists.get_by_title(document_library_name).root_folder.folders.get_by_url(folder_name).folders.get_by_url(sub_folder)

    # Open the local file in binary mode and upload it to SharePoint
    with open(local_file_path, "rb") as file_content:
        try:
            target_file_name = local_file_path.split(r"/")[-1]
            folder.upload_file(target_file_name, file_content).execute_query()
        except Exception as e:
            target_file_name = local_file_path.split("\\")[-1]
            folder.upload_file(target_file_name, file_content).execute_query()


    print(f"File '{target_file_name}' has been uploaded to SharePoint.")

            # start of sharepoint
    print(f'now uploading to sharepoint performance folder')
    from office365.sharepoint.client_context import ClientContext

    # SharePoint folder and local file details
    document_library_name = "Documents"
    folder_name = "Prod_CSVs"
    sub_folder = "performance"
    local_file_path = filename3

    # Initialize the ClientContext
    ctx = ClientContext(sharepoint_site_url).with_user_credentials(username, password)

    # Get the folder
    folder = ctx.web.lists.get_by_title(document_library_name).root_folder.folders.get_by_url(folder_name).folders.get_by_url(sub_folder)

    # Open the local file in binary mode and upload it to SharePoint
    with open(local_file_path, "rb") as file_content:
        try:
            target_file_name = local_file_path.split(r"/")[-1]
            folder.upload_file(target_file_name, file_content).execute_query()
        except Exception as e:
            target_file_name = local_file_path.split("\\")[-1]
            folder.upload_file(target_file_name, file_content).execute_query()


    print(f"File '{target_file_name}' has been uploaded to SharePoint.")
