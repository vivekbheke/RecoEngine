/*
 * Copyright 2014 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "as is" Basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


----------------------------------------------------------------------------------------------------
/*
 * This file contains the macros that can be used in your pigscript in order to modify your data
 * before using it to build a recommendation graph
 */
----------------------------------------------------------------------------------------------------



/*
 * This is used for adding metadata to existing item to item links. 
 * This does not recalculate the weight of the item-item connections.
 *
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float) }
 *      metadata: { (item:chararray, metadata_field:chararray) }
 * Output:
 *      ii_links_metadata: { (item_A:chararray, metadata_A:chararray, item_B:chararray, 
 *                            metadata_B:chararray, weight:float ) }
 */
define recsys__AddMetadataToItemItemLinks(ii_links, metadata) returns ii_links_metadata {

    ii_links_metadata_1 = foreach (join $ii_links by item_A, $metadata by item) generate 
                                item_A, item_B, weight, metadata_field as metadata_A;

    $ii_links_metadata  = foreach (join ii_links_metadata_1 by item_B, $metadata by item) generate
                                item_A, item_B, weight, metadata_A, metadata_field as metadata_B;
};

/*
 * This is used to remove bots that could exist in user item signals.
 * This is used before the recommender algorithm to avoid having recommendations based off of the bot's behaviour.
 *
 * A user is considered a bot if they have a certain amount of item links which exceed a determined threshold.
 *
 * Input:
 *      user_item_signal: { (user:chararray, item:chararray, weight:float) }
 *      threshold: int 
 * Output:
 *      users_clean: { (user:chararray, item:chararray, weight:float) }
 */
define recsys__RemoveBots(user_item_signal, threshold) returns users_clean {

    users_size   = foreach (group $user_item_signal by user) generate
                                          group as user,
                        SIZE($user_item_signal) as num_signals;

    users_normal = filter users_size by num_signals < $threshold;

    $users_clean = foreach (join users_normal by user, $user_item_signal by user) generate
                        users_normal::user as user,
                                      item as item,
                                    weight as weight;
};
