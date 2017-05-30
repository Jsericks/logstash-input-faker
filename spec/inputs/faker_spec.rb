
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/faker"

describe LogStash::Inputs::Faker do
  it_behaves_like "an interruptible input plugin" do
    let(:config) { { } }
  end

  it "should generate a message and remove the Faker calls" do
    plg = LogStash::Inputs::Faker.new
    plg.count = 1
    plg.add_faker_field = {
                            "first_name" => "Name.first_name",
                            "last_name" => "Name.last_name"
                          }
    plg.splitable_field = "data"
    plg.add_splitable_field = {
                                "test" => "field"
                              }
    plg.add_splitable_faker_field = {
                                      "[name][testing]" => "Name.first_name"
                                    }
    plg.splitable_field_count = 10

    event = plugin_input(plg) do |result|
      result.pop
    end

    insist { event }.is_a? LogStash::Event
    insist { event.get("data").size } == 10
    insist { event.get("data").map{|e| e["test"] } }.all? { |v| v == "field" }
    insist { event.get("first_name") } != "Name.first_name"
    insist { event.get("last_name") } != "Name.last_name"
  end

  it "should generate a message with nested fields and the added_field" do
    plg = LogStash::Inputs::Faker.new
    plg.count = 1
    plg.add_faker_field = {
                            "[name][first_name]" => "Name.first_name",
                            "[name][last_name]" => "Name.last_name"
                          }
    plg.add_field = { "[name][middle_name]" => "Jimmy" }

    event = plugin_input(plg) do |result|
      result.pop
    end

    insist { event }.is_a? LogStash::Event
    insist { event.get("[name][middle_name]") } == "Jimmy"
    insist { event.get("[name][first_name]") } != "Name.first_name"
    insist { event.get("[name][last_name]") } != "Name.last_name"
  end

  it "should generate 100 messages" do
    plg = LogStash::Inputs::Faker.new
    plg.count = 100
    plg.add_faker_field = {
                            "first_name" => "Name.first_name",
                            "last_name" => "Name.last_name"
                          }
    events = plugin_input(plg) do |result|
      100.times.map{result.pop}
    end

    insist { events.size } == 100
  end
end
