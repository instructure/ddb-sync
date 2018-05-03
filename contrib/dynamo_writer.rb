require 'aws-sdk-dynamodb'
require 'securerandom'

dynamodb = Aws::DynamoDB::Client.new(region: 'us-west-2')

def create_item(seq: 5)
  {
    Artist: "Here's a name #{seq}",
    SongTitle: "Here's a title",
    AlbumTitle: "Here's an album title",
    DATA: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.'
  }
end

i = 0
while i < 1000
  params = {
    table_name: 'ddb-sync-source',
    item: create_item(seq: SecureRandom.uuid)
  }
  begin
    puts "Writing item ##{i+1}"
    _ = dynamodb.put_item(params)
  rescue Aws::DynamoDB::Errors::ServiceError => error
    puts error.message
  end
  i += 1
  sleep 2
end
