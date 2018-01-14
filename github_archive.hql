create scheam github_archive;

CREATE TABLE create_evt (   
	id string,
	created_at string,                  
	is_repo_public boolean,             
	repo_id bigint,  
	repo_name string,
	repo_url string, 
	actor_login string,                 
	actor_id bigint, 
	actor_url string,
	org_id bigint,   
	org_url string,  
	org_login string,
	ref_type string, 
	master_branch string,               
	description string,                 
	head string,     
	size bigint
)
stored as parquet;