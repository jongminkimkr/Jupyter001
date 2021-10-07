from pathlib import Path




import shutil


import subprocess


import os


import json





from log_collector.log import logger









def call_cpdctl(*args, context=None, convert_to_json=True):







   cpdctl_command = ["cpdctl"]







   if shutil.which("cpdctl") is None:


       path = str(Path(__file__).parent.absolute())







       cpdctl_binary = path + "/cpdctl"


       cpdctl_command = [cpdctl_binary]







   options = ["--output", "json"]







   context_env = os.environ.get("context")







   if context:


       cpdctl_command += ["--context", context]


   elif context_env:


       cpdctl_command += ["--context", context_env]







   cpdctl_command += list(args) + options







   debug_command = (" ").join([str(i) for i in cpdctl_command])







   # print(debug_command)


   logger.debug(debug_command)







   try:


       cmd_out = subprocess.run(


           cpdctl_command,


           check=True,


           stdout=subprocess.PIPE,


           stderr=subprocess.STDOUT,


           universal_newlines=True,


       )


       # print(cmd_out.stdout)







       if cmd_out.returncode != 0:


           logger.error(cmd_out.stderr)


           raise Exception(f"cpdctl call failed: {debug_command}")







       if convert_to_json:


           cmd_out_as_json = json.loads(cmd_out.stdout)


           return cmd_out_as_json


       else:


           return cmd_out.stdout


   except subprocess.CalledProcessError as e:


       logger.error("Error calling cpdctl:" + e.output)







       if "502" in e.output:


           raise Exception("Bad gateway, CPD cluster most likely down")







   except json.decoder.JSONDecodeError as ex:


       logger.error("String could not be converted to JSON")


       raise Exception("Error converting cpdctl result to JSON", ex)












def get_all_spaces():


   logger.info("Get spaces")


   list_limit = 100


   return call_cpdctl("space", "list", "--limit", str(list_limit))












def get_space_name_by_id(space_id):


   space_name = call_cpdctl(


       "space",


       "list",


       "--id",


       str(space_id),


       "--jmes-query",


       "resources[0].entity.name",


   )







   return space_name












def list_deployment_jobs(space_id):







   deployment_jobs = call_cpdctl(


       "ml", "deployment-job", "list", "--space-id", space_id


   )







   return deployment_jobs












def search_all_assets_for_type(space_id):


   logger.debug(f"Search assets for data asset")


   asset_type = "data_asset"


   assets = call_cpdctl(


       "asset",


       "search",


       "--space-id",


       space_id,


       "--type-name",


       asset_type,


       "--query",


       "*:*",


   )


   if assets:


       # logger.info(f"Got asset ids for " + asset_type)


       return assets


   else:


       logger.error("Error getting asset ids")












def delete_asset(asset, space_id):


   # logger.info(f"Get asset type of '{asset_id}' ")


   asset_name = asset["metadata"]["name"]


   asset_id = asset["metadata"]["asset_id"]


   logger.info("Deleting " + asset_name)


   try:


       call_cpdctl("asset", "delete", "--asset-id", asset_id, "--space-id", space_id)


       logger.info("Deletion successful\n\n")


       return


   except Exception as e:


       logger.exception(


           "Could not delete asset - might be due unsufficient permission. ", e


       )



