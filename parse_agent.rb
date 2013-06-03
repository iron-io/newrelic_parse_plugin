require 'yaml'
require 'parse-ruby-client'
require 'base64'
require 'iron_cache'
# Requires manual installation of the New Relic plaform gem
# https://github.com/newrelic-platform/iron_sdk
require 'newrelic_platform'

# Un-comment to test/debug locally
# def config; @config ||= YAML.load_file('./parse_agent.config.yml'); end

# Setup
@test_mode = config['test_mode']

Parse.init(:api_key => config['parse']['api_key'],
           :application_id => config['parse']['app_id'])

# Configure NewRelic client
@new_relic = NewRelic::Client.new(:license => config['newrelic']['license'],
                                  :guid => config['newrelic']['guid'],
                                  :version => config['newrelic']['version'])

# Configure IronCache client
begin
  @cache = IronCache::Client.new(config['iron']).cache("newrelic-parse-agent")
rescue Exception => err
  abort 'Iron.io credentials are wrong.'
end

# Helpers

def duration(from, to)
  dur = from ? (to - from).to_i : 3600

  dur > 3600 ? 3600 : dur
end

def up_to(to = nil)
  if to
    @up_to = Time.at(to.to_i).utc
  else
    @up_to ||= Time.now.utc
  end
end

def processed_at(processed = nil)
  if processed
    @cache.put('previously_processed_at', processed.to_i)

    @processed_at = Time.at(processed.to_i).utc
  elsif @processed_at.nil?
    item = @cache.get 'previously_processed_at'
    min_prev_allowed = (up_to - 3600).to_i

    at = if item && item.value.to_i > min_prev_allowed
           item.value
         else
           min_prev_allowed
         end

    @processed_at = Time.at(at).utc
  else
    @processed_at
  end
end

def users_count(from = nil, thru = nil)
  query = Parse::Query.new('_User')
  if from
    query = query.greater_than('CreatedAt', from.to_i)
  end
  if thru
    query = query.less_eq('CreatedAt', thru.to_i)
  end

  begin
    query.count.get.count
  rescue Exception => err
    if err.message =~ /unauthorized/
      abort 'Seems Parse credentials are wrong.'
    else
      abort("Error happened while getting data from Parse. " +
            "Error message: '#{err.message}'.")
    end
  end
end

def cached_total_users(count = nil)
  if count
    @cache.put('previously_total_users_count', count.to_i)

    @cached_total_users = count.to_i
  elsif @cached_total_users.nil?
    item = @cache.get 'previously_total_users_count'

    @cached_total_users = if item && !item.value.nil?
                            item.value
                          elsif @test_mode # first test_mode launch
                            0
                          else # first normal launch
                            users_count(nil, processed_at)
                          end.to_i
  end

  @cached_total_users
end

# Processing

users = {}
if @test_mode
  # [2, 7] new users
  users[:new] = 2 + Random.rand(6)
  # prev_total + new - [0..7]
  users[:total] = cached_total_users + users[:new] - Random.rand(8)
  users[:total] = 0 if users[:total] < 0
else
  users[:new] = users_count(processed_at, up_to)
  users[:total] = users_count
end
users[:deleted] = cached_total_users + users[:new] - users[:total]

if @test_mode
  puts "From: #{processed_at}; thru: #{up_to}"
  puts "Users: #{users.inspect}"
end

collector = @new_relic.new_collector
component = collector.component 'Parse'

component.add_metric('Users/New', 'users', users[:new])
component.add_metric('Users/Deleted', 'users', users[:deleted])
component.add_metric('Users/Total', 'users', users[:total])

component.options[:duration] = duration(processed_at, up_to)

begin
  # Submit data to New Relic
  collector.submit
rescue Exception => err
  #restore_stderr
  if err.message.downcase =~ /http 403/
    abort "Seems New Relic's license key is wrong."
  else
    abort("Error happened while sending data to New Relic. " +
          "Error message: '#{err.message}'.")
  end
end

# Update cached data
processed_at up_to
cached_total_users users[:total]
