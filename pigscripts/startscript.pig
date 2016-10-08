/**
 *  This script is an example recommender (using made up data) showing how you might extract 
 *  multiple user-item signals from your data.  Here, we extract one signal based on
 *  user purchase information and another based on a user adding a movie to their wishlist.  We
 *  then combine those signals before running the Mortar recommendation system to get item-item
 *  and user-item recommendations.
 */
import 'recommenders.pig';

%default INPUT_PATH_PURCHASES '../data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '../data/retail/wishlists.json'
%default OUTPUT_PATH '../data/retail/out'
%default PIGGYBANKPATH 'udfs\java\{datafu-1.2.0.jar,recsys-udfs.jar,trove4j-3.0.3.jar}'



REGISTER $PIGGYBANKPATH;
/******* Load Data **********/

--Get purchase signals
purchase_input = load '$INPUT_PATH_PURCHASES' using org.apache.pig.piggybank.storage.JsonLoader(
                    'row_id: int, 
                     movie_id: chararray, 
                     movie_name: chararray, 
                     user_id: chararray, 
                     purchase_price: int');

--Get wishlist signals
wishlist_input =  load '$INPUT_PATH_WISHLIST' using org.apache.pig.piggybank.storage.JsonLoader(
                     'row_id: int, 
                      movie_id: chararray, 
                      movie_name: chararray, 
                      user_id: chararray');



/******* Convert Data to Signals **********/

-- Start with choosing 1 as max weight for a signal.
purchase_signals = foreach purchase_input generate
                        user_id    as user,
                        movie_name as item,
                        1.0        as weight;


-- Start with choosing 0.5 as weight for wishlist items because that is a weaker signal than
-- purchasing an item.
wishlist_signals = foreach wishlist_input generate
                        user_id    as user,
                        movie_name as item,
                        0.5        as weight;

user_signals = union purchase_signals, wishlist_signals;


/******* Use Mortar recommendation engine to convert signals to recommendations **********/

item_item_recommendation = recsystem__GetItemItemRecommendations(user_signals);


user_item_recommendation = recsystem__GetUserItemRecommendations(user_signals, item_item_recommendation);


/******* Store recommendations **********/

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recommendation;
rmf $OUTPUT_PATH/user_item_recommendation;

store item_item_recs into '$OUTPUT_PATH/item_item_recommendation' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recommendation' using PigStorage();

