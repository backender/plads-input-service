{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Monad
import qualified Control.Exception                  as E

import           Data.Aeson
import           Data.Aeson.Lens
import           Data.Maybe
import qualified Data.Text                          as T
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL

import           Network.Wreq

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

import           Database.MySQL.Simple

type MediaId = Int

data NewMediaMessage = NewMediaMessage { nmId :: Integer,
                                         nmSrc :: T.Text
                                       } deriving (Show)

data MediaConfig = VideoConfig {} | AudioConfig {}
                   deriving(Show)

data CreateInputResponse = CreateInputResponse { mediaId :: Integer,
                                                 inputId :: Maybe Integer,
                                                 status :: Maybe T.Text,
                                                 thumbnailUrl :: Maybe T.Text,
                                                 duration :: Maybe Integer,
                                                 mediaType :: Maybe T.Text
                                                 --mediaConfigurations :: Maybe MediaConfig
                                               } deriving (Show)

instance FromJSON NewMediaMessage where
 parseJSON (Object v) =
    NewMediaMessage <$> v .: "id"
                    <*> v .: "src"
 parseJSON _ = mzero

instance ToJSON CreateInputResponse where
  toJSON (CreateInputResponse mediaId (Just inputId) (Just status) (Just thumb) _ _) =
      object respHeader
      where respHeader = [ "mediaId" .= mediaId,
                           "inputId" .= inputId,
                           "status" .= status,
                           "thumbnailUrl" .= thumb ]

api :: String -> String
--api s = "http://portal.bitcodin.com/api" ++ s --PROD
api s = "https://private-anon-d220f1902-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createInput :: T.Text -> IO (Response BL.ByteString)
createInput src = do
    let payload = encode $ object ["type" .= ("url" :: T.Text), "url" .= src]
    postWith opts (api "/input/create") payload --TODO: handle exception!

decodeCTPayload :: C8.ByteString -> Maybe NewMediaMessage
decodeCTPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO (Either String NewMediaMessage)
handleConsume e =
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return $ Left $ "[INFO] " ++ show err
                      _                                         -> return $ Left $ "[ERROR] " ++ show err
      (Right m) -> do
          print $ BL.fromStrict $ messagePayload m
          case decodeCTPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ show (messagePayload m)
            Just m -> return $ Right m

handleResponse :: Integer -> Response BL.ByteString -> IO (Either String CreateInputResponse)
handleResponse cid r =
    case code of
        201 ->
          case status inputResponse of
            Just s -> case s of
              "CREATED" -> return $ Right inputResponse
              _ -> return $ Left "[ERROR] input was not created"
            Nothing -> return $ Left "[ERROR] could not determine input status."
        _ -> return $ Left $ handleErrorResponse code
    where
      code = r ^. responseStatus . statusCode
      rb x t = r ^? responseBody . key x . t
      inputResponse = CreateInputResponse cid
                                          (rb "inputId" _Integer)
                                          (rb "status" _String)
                                          (rb "thumbnailUrl" _String)
                                          (r ^? responseBody . key "mediaConfigurations" . nth 0 . key "duration" . _Integer)
                                          (r ^? responseBody . key "mediaConfigurations" . nth 0 . key "type" . _String)

updateMedia :: Connection -> MediaId -> (Integer, T.Text, Integer, T.Text) -> IO ()
updateMedia conn mediaId (status, thumbnail, duration, mediaType) =
    check =<< E.try (execute conn q args)
                  where q = "update media set status=?, thumb_url=?, duration=?, type=? where id =?" :: Query
                        args = [(T.pack . show) status, thumbnail, (T.pack . show) duration, mediaType, (T.pack . show) mediaId]
                        check (Left (E.SomeException e)) = putStrLn $ "[ERROR] Updating media with id (" ++ show mediaId ++ ") not successfull: " ++ show e
                        check (Right r) = putStrLn $ "[Info] Media with id " ++ show mediaId ++ " successfully updated: " ++ show r


handleErrorResponse :: Int -> String
handleErrorResponse e =
  case e of
    404 -> "Not found."

produce :: KafkaProduceMessage -> IO (Maybe KafkaError)
produce message = do
  let partition = 0
      host = "localhost:9092"
      topic = "mediaInput"
      kafkaConfig = []
      topicConfig = []
  withKafkaProducer kafkaConfig topicConfig
                    host topic
                    $ \kafka topic -> produceMessage topic (KafkaSpecifiedPartition partition) message

getConnection :: IO Connection
getConnection = connect defaultConnectInfo {  connectHost = "127.0.0.1",
                                              connectPassword = "password",
                                              connectDatabase = "plads" }

main :: IO ()
main = do
    conn <- getConnection

    let partition = 0
        host = "localhost:9092"
        topic = "newMedia"
        kafkaConfig = []
        topicConfig = []
    withKafkaConsumer kafkaConfig topicConfig
                      host topic
                      partition
                      KafkaOffsetStored
                      $ \kafka topic -> forever $ do
    c <- handleConsume =<< consumeMessage topic partition 1000
    case c of
      Right newMedia -> do
        let mediaId = nmId newMedia
        let mediaSrc = nmSrc newMedia
        inputResponse <- createInput mediaSrc
        respHandle <- handleResponse mediaId inputResponse
        case respHandle of
          Right input -> do
            _ <- updateMedia conn (fromIntegral mediaId) (1, fromJust $ thumbnailUrl input, fromJust $ duration input, fromJust $ mediaType input)
            prod <- produce $ KafkaProduceMessage $ BL.toStrict $ encode input
            case prod of
              Nothing -> putStrLn $ "[INFO] Produced Input: " ++ show input
              Just e -> print e
          Left e -> putStrLn e
      Left e -> putStrLn e
