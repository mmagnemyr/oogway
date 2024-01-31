# Python script to split a  and write each entry into a separate row in a file

input_ = "connection_id , iccid , imsi , msisdn , imei , mdn , esn , meid , eid , cdp_id , native_provider_acc_id , provider ,  apn , rate_plan , price_zone , sub_zone , operator , country , data_volume_kb , billable_data_volume_kb , data_volume_kb_p , data_session_count , zero_byte_session_count , sms_mo , sms_mo_billable , sms_mo_p , sms_mt , sms_mt_billable , sms_mt_p , sms_total , sms_total_billable , sms_total_p , voice_mo , voice_mo_billable , voice_mo_p , voice_mt , voice_mt_billable , voice_mt_p , voice_total , voice_total_billable , voice_total_p , currency_code , billing_cycle , billing_cycle_start_day , last_fully_sync_day , remaining_days_in_cycle , subscription_charge , proration_index , activation_fee , tech_surcharge , data_overage_charge_pooled , data_overage_charge_pooled_p , data_overage_charge_non_pooled , data_overage_charge_non_pooled_p , sms_overage_charge_pooled , sms_overage_charge_pooled_p , sms_charge_non_pooled , sms_charge_non_pooled_p , voice_overage_charge_pooled , voice_overage_charge_pooled_p , voice_charge_non_pooled , voice_charge_non_pooled_p , pausd_subscription_charge , monthly_subscription_charge_reconciliation , paused_subscription_charge , monthly_paused_subscription_charge_reconciliation , year , month , cdpaccount , cdpname , dateserial "

# Split the  into a list of entries
entries = [entry.strip() for entry in input_.split(',')]

# Write the entries to a file
with open('columns.txt', 'w') as file:
    for entry in entries:
        file.write(entry + '\n')

print("File created successfully.")