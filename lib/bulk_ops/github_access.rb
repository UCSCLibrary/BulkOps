require "octokit"
require "socket"
require "securerandom"
require 'base64'

class BulkOps::GithubAccess

  ROW_OFFSET = 2
  SPREADSHEET_FILENAME = 'metadata.csv'
  OPTIONS_FILENAME = 'configuration.yml'

  attr_accessor :name

  def self.auth_url user
    "#{Octokit.web_endpoint}login/oauth/authorize?client_id=#{client_id}&redirect_uri=#{redirect_endpoint(user)}&state=#{state(user)}&scope=repo"
  end
  
  def self.username user
    return false unless (cred = BulkOps::GithubCredential.find_by(user_id: user))
    return false unless (token = cred.oauth_token)
    Octokit::Client.new(access_token: token).user[:login] rescue false
  end
  
  def self.valid_state?(astate,user_id=nil)
    return (astate === state(user_id))
  end

  def self.state user_id=nil
    user_id = current_user.id if user_id.nil?
    user_id = user_id.id if user_id.is_a? User
    user_id = user_id.to_i if user_id.is_a? String
    cred = BulkOps::GithubCredential.find_by(user_id: user_id) || BulkOps::GithubCredential.create({user_id: user_id, state: SecureRandom.hex })
    cred.state
  end
  
  def self.auth_token user_id=nil
    user_id = current_user.id if user_id.nil?
    user_id = user_id.id if user_id.is_a? User
    user_id = user_id.to_i if user_id.is_a? String
    return false unless (cred = BulkOps::GithubCredential.find_by(user_id: user_id))
    return cred.auth_token || false
  end
  
  def self.set_auth_token!(code,user_id=nil)
    user_id = current_user.id if user_id.nil?
    user_id = user_id.id if user_id.is_a? User
    user_id = user_id.to_i if user_id.is_a? String
    cred = BulkOps::GithubCredential.find_by(user_id: user_id)
    result = HTTParty.post("https://github.com/login/oauth/access_token", 
                           body: {client_id: client_id,
                                  client_secret: client_secret,
                                  code: code,
                                  accept: "json"}.to_json, 
                           headers: { 'Content-Type' => 'application/json', 
                                      'Accept' => 'application/json'})
    cred.update(oauth_token: result.parsed_response["access_token"])
  end
  
  def self.client_id
    YAML.load_file("#{Rails.root.to_s}/config/github.yml")[Rails.env]["client_id"]
  end

  def self.client_secret
    YAML.load_file("#{Rails.root.to_s}/config/github.yml")[Rails.env]["client_secret"]
  end
  
  def self.webhook_token
    YAML.load_file("#{Rails.root.to_s}/config/github.yml")[Rails.env]["client_secret"]
  end
  
  def self.redirect_endpoint user
    host = Socket.gethostname
    host = "localhost" if Rails.env.development? or Rails.env.test?
    "http://#{host}/bulk_ops/authorize/#{User.first.id}"
  end

  def self.create_branch! name 
    self.new(name).create_branch! 
  end

  def self.add_file name, file_path, file_name = nil, message: false
    self.new(name).add_file file_path, file_name, message: message
  end

  def self.add_contents name, file_name, contents, message: false
    self.new(name).add_contents file_name, contents, message
  end

  def self.load_options name, branch: nil
    self.new(name).load_options branch: branch
  end

  def self.load_metadata branch:, return_headers: false
    self.new(branch).load_metadata return_headers: return_headers
  end
  
  def self.update_options name, options, message: false
    self.new(name).update_options options, message=false
  end

  def self.list_branches
    self.new.list_branches
  end

  def self.list_branch_names user
    self.new.list_branch_names
  end

  def initialize(newname="dummy", user = nil)
    @name = newname.parameterize
    @user = user
  end

  def create_branch!
    client.create_ref repo, "heads/#{name}", current_master_commit_sha
  end

  def delete_branch!
    return false unless list_branch_names.include? name
    client.delete_branch repo, name
  end

  def add_file file_path, file_name = nil, message: nil
    file_name ||= File.basename(file_path)
    #    unless (file_name.downcase == "readme.md") || (file_name.downcase.include? "#{name}/")
    file_name = File.join(name, file_name) unless file_name.downcase.include? "#{name.downcase}/"
    message ||= "adding file #{file_name} to github branch #{name}"
    client.create_contents(repo, file_name, message, file: file_path, branch: name)
  end

  def add_contents file_name, contents, message=false
    message ||= "adding file #{file_name} to github branch #{name}"
    begin
      client.create_contents(repo, file_name, message, contents, branch: name)
    rescue Octokit::UnprocessableEntity
      sha = get_file_sha(file_name)
      client.update_contents(repo, file_name, message, sha, contents, branch: name)
    end
  end

  def add_new_spreadsheet file_contents, message=false
    add_contents(spreadsheet_path, file_contents, message)
  end

  def list_branches
    client.branches(repo).select{|branch| branch[:name] != "master"}
  end

  def list_branch_names
    list_branches.map{|branch| branch[:name]}
  end

  def update_spreadsheet file, message: false
    message ||= "updating metadata spreadsheet through hyrax browser interface."
    sha = get_file_sha(spreadsheet_path)
    file = File.new(file) if file.is_a?(String) && Pathname(file).exist?
    client.update_contents(repo, spreadsheet_path, message, sha, file.read, branch: name)
  end

  def update_options options, message: false
    message ||= "updating metadata spreadsheet through hyrax browser interface."
    sha = get_file_sha(options_path)
    client.update_contents(repo, options_path, message, sha, YAML.dump(options), branch: name)
  end

  def load_options branch: nil
    branch ||= name
    YAML.load(Base64.decode64(get_file_contents(options_path, ref: branch)))
  end

  def load_metadata branch: nil, return_headers: false
    branch ||= name
    CSV.parse(Base64.decode64(get_file_contents(spreadsheet_path, ref: branch)), {headers: true, return_headers: return_headers})
  end

  def log_ingest_event log_level, row_number, event_type, message, commit_sha = nil
    commit_sha ||= current_branch_commit_sha
    #TODO WRITE THIS CODE
  end

  def create_pull_request message: false
    begin
      message ||= "Apply update #{name} through Hyrax browser interface"
      pull = client.create_pull_request(repo, "master", name, message)
      pull["number"]
    rescue Octokit::UnprocessableEntity
      return false
    end
  end

  def can_merge?
    return true
  end

  def merge_pull_request pull_id, message: false
    client.merge_pull_request(repo, pull_id, message)
  end

  def get_metadata_row row_number
    @current_metadata ||= load_metadata
    @current_metadata[row_number - ROW_OFFSET]
  end
  
  def get_past_metadata_row commit_sha, row_number
    past_metadata = Base64.decode64( client.contents(repo, path: filename, ref: commit_sha) )
    past_metadata[row_number - ROW_OFFSET]
  end

  def get_file filename
    client.contents(repo, path: filename, ref: name)
  end

  def get_file_contents filename, ref: nil
    ref ||= name
    client.contents(repo, path: filename, ref: ref)[:content]
  end

  def get_file_sha filename
    client.contents(repo, path: filename, ref: name)[:sha]
  end

  def repo
    github_config["repo"]
  end

  def spreadsheet_path
    "#{name}/#{SPREADSHEET_FILENAME}"
  end

  private

  def options_path
    "#{name}/#{OPTIONS_FILENAME}"
  end

  def current_master_commit_sha
    client.branch(repo,"master").commit.sha
  end

  def current_branch_commit_sha
    client.branch(repo, name).commit.sha
  end

  def client
    return @client if @client
    return default_client if @user.nil?
    return false unless (cred = BulkOps::GithubCredential.find_by(user_id: @user.id))
    return false unless (token = cred.oauth_token)
    client ||= Octokit::Client.new(access_token: token)
    return false unless client.user[:login].is_a? String
    @client = client
  end

  def default_client
    @client ||= Octokit::Client.new(login: github_config["default_user"], password: github_config["default_password"])
  end

  def github_config
    @github_config ||=  YAML.load_file("#{Rails.root.to_s}/config/github.yml")[Rails.env]
  end


end
