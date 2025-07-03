import sys
import json
import dpath
import argparse
import yaml_loader
import confluent_kafka

from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException, ConsumerGroupState
from pathlib import Path


# JSON module can't serialise sets by default, so we
# create our own serialiser.
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def list_consumer_groups_and_topics(conf, group, output_file='/dev/stdout'):
   """
   Iterates over all consumer groups and lists the topics 
   being consumed by each group.
   """
   admin_client_conf = {
      'bootstrap.servers': dpath.get(conf,'confluent/cluster/bootstrap_endpoint'),
      'security.protocol': 'SASL_SSL',
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': dpath.get(conf,'confluent/cluster/api_key'),
      'sasl.password': dpath.get(conf,'confluent/cluster/api_secret')
   }
   admin_client = None # Initialize to None for the finally block

   topic_to_group = {}
   group_to_topic = {}

   try:
      admin_client = AdminClient(admin_client_conf)

      # List all consumer groups
      print("Fetching consumer groups...")
      list_groups_result = admin_client.list_consumer_groups(request_timeout=20).result()

      if list_groups_result.errors:
         print(f"Errors occurred while listing consumer groups: {list_groups_result.errors}")
         # You might want to handle specific errors here
         return

      consumer_group_ids = [group.group_id for group in list_groups_result.valid]

      if group is not None:
         if group in consumer_group_ids:
            consumer_group_ids = [ group ]
         else:
            consumer_group_ids = []
         

      if not consumer_group_ids:
         print("No consumer groups found.")
         return

      print(f"Found {len(consumer_group_ids)} consumer groups.")

      # Describe each consumer group to get topic assignments
      print("\nDescribing consumer groups and their topic assignments:")
      describe_groups_futures = admin_client.describe_consumer_groups(consumer_group_ids)


      for group_id, future in describe_groups_futures.items():
         try:
            description = future.result()  # Get the ConsumerGroupDescription object
            print(f"\n--- Consumer Group: {group_id} ---")
            print(f"  State: {description.state.name}") # e.g., STABLE, EMPTY, REBALANCING

            if description.members:
               consumed_topics = set()
               for member in description.members:
                  if member.assignment and member.assignment.topic_partitions:
                     for tp in member.assignment.topic_partitions:
                        consumed_topics.add(tp.topic)

                        # Update our group->topic mapping
                        gtt = group_to_topic.get(group_id,set())
                        gtt.add(tp.topic)
                        group_to_topic[group_id] = gtt

                        # Update our topic->group mapping
                        ttg = topic_to_group.get(tp.topic,set())
                        ttg.add(group_id)
                        topic_to_group[tp.topic] = ttg
      

               if consumed_topics:
                  print(f"  Consumed Topics: {', '.join(sorted(list(consumed_topics)))}")
               else:
                  print("  No topics currently assigned to members.")
            else:
               print("  No active members in this group.")

         except KafkaException as e:
            print(f"  Error describing group {group_id}: {e}")
         except Exception as e:
            print(f"  An unexpected error occurred for group {group_id}: {e}")

   except KafkaException as e:
      print(f"Kafka Admin Client error: {e}")
   except Exception as e:
      print(f"An unexpected error occurred: {e}")
   finally:
      
      topics_file = "{}-topics{}".format(Path(output_file).stem, Path(output_file).suffix)
      groups_file = "{}-groups{}".format(Path(output_file).stem, Path(output_file).suffix)

      
      if len(topic_to_group) > 0:
         print("Writing Topic->Groups mapping to {} file.".format(topics_file))
         with open(topics_file,'w') as f:
            json.dump(topic_to_group, f, cls=SetEncoder)

      if len(group_to_topic) > 0:
         print("Writing Group->Topics mapping to {} file.".format(groups_file))
         with open(groups_file,'w') as f:
            json.dump(group_to_topic, f, cls=SetEncoder)



def main():
   parser = argparse.ArgumentParser()

   parser.add_argument('--group', '-g',
      type=str,
      nargs='?',
      help='Consumer group to query (default is all groups)')

   # I want my own optional/required group ordering
   req = parser.add_argument_group('required arguments')

   req.add_argument('--config_file', '-f',
   type=str,
   required=True,
   help='Configuration file')

   req.add_argument('--output_file', '-o',
   type=str,
   required=True,
   help='File to write group/topic info to (default is stdout)')

   if len(sys.argv) == 1:
      parser.print_help(sys.stderr)
      sys.exit(1)

   args = parser.parse_args()

   if args.config_file is None:
      print("No config_file provided - not cool.")
      sys.exit(1)
   else:
      config_file = yaml_loader.load_config(args.config_file)
      list_consumer_groups_and_topics(
         conf=config_file,
         group=args.group,
         output_file=args.output_file)



if __name__ == "__main__":
   main()
