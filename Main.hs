{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Concurrent                           ( threadDelay )
import Control.Monad

import           Data.Aeson
import           Data.Aeson.Lens
import           Data.Maybe
import qualified Data.Text                          as T
import qualified Data.Map                           as M
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL

import           Network.Wreq

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

data CampaignTopicMessage = CampaignTopicMessage { id :: Int,
                                                   src :: T.Text
                                                 } deriving (Show)

data CreateInputResponse = CreateInputResponse { inputId :: Maybe Integer,
                                                 status :: Maybe T.Text,
                                                 thumbnailUrl :: Maybe T.Text
                                                 --type :: Maybe T.Text
                                               } deriving (Show)

instance FromJSON CampaignTopicMessage where
 parseJSON (Object v) =
    CampaignTopicMessage <$> v .: "id"
                         <*> v .: "src"
 parseJSON _ = mzero

instance ToJSON CreateInputResponse where
  toJSON (CreateInputResponse (Just inputId) (Just status) (Just thumb)) =
      object ["inputId" .= inputId, "status" .= status, "thumbnailUrl" .= thumb]

api :: String -> String
--api s = "http://portal.bitcodin.com/api" ++ s --PROD
api s = "http://private-anon-b5423cd68-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createInput :: T.Text -> IO (Response BL.ByteString)
createInput src = do
    let payload = encode $ object ["inputType" .= ("url" :: T.Text), "url" .= src]
    postWith opts (api "/input/create") payload

decodeCTPayload :: C8.ByteString -> Maybe CampaignTopicMessage
decodeCTPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO (Either String CampaignTopicMessage)
handleConsume e = do
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return $ Left $ "[INFO] " ++ (show err)
                      _                                         -> return $ Left $ "[ERROR] " ++ (show err)
      (Right m) -> do
          print $ BL.fromStrict $ messagePayload m
          case decodeCTPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ (show $ messagePayload m)
            Just m -> return $ Right m

handleResponse :: Response BL.ByteString -> IO (Either String CreateInputResponse)
handleResponse r = do
    case code of
        201 -> do
          print $ fromJust $ status inputResponse
          case fromJust $ status inputResponse of
            "CREATED" -> return $ Right inputResponse
            _ -> return $ Left "[ERROR] input was not created"
        _ -> return $ Left $ handleErrorResponse code
    where
      code = (r ^. responseStatus . statusCode)
      rb = \x t -> r ^? responseBody . key x . t
      inputResponse = CreateInputResponse (rb "inputId" _Integer )
                                          (rb "status" _String)
                                          (rb "thumbnailUrl" _String)

handleErrorResponse :: Int -> String
handleErrorResponse e = do
  case e of
    404 -> "Not found."

produce :: KafkaProduceMessage -> IO (Maybe KafkaError)
produce message = do
  let partition = 0
      host = "localhost:9092"
      topic = "campaignInput"
      kafkaConfig = []
      topicConfig = []
  withKafkaProducer kafkaConfig topicConfig
                    host topic
                    $ \kafka topic -> do
  produceMessage topic (KafkaSpecifiedPartition partition) message


main :: IO ()
main = do
    let partition = 0
        host = "localhost:9092"
        topic = "campaignRequest"
        kafkaConfig = []
        topicConfig = []
    withKafkaConsumer kafkaConfig topicConfig
                      host topic
                      partition
                      KafkaOffsetStored
                      $ \kafka topic -> forever $ do
    c <- handleConsume =<< consumeMessage topic partition 1000
    case c of
      Right m -> do
        resp <- handleResponse =<< (createInput $ src m)
        case resp of
          Right input -> do
            prod <- produce $ KafkaProduceMessage $ BL.toStrict $ encode input
            case prod of
              Nothing -> putStrLn "produced"
              Just e -> putStrLn $ show e
          Left e -> putStrLn e
      Left e -> putStrLn e
