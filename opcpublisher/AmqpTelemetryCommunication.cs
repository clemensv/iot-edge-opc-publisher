namespace OpcPublisher
{
    using Microsoft.Azure.Devices;
    using Opc.Ua;
    using System;
    using static OpcApplicationConfiguration;
    using static Program;

    /// <summary>
    /// Class to handle all IoTHub communication.
    /// </summary>
    public class AmqpTelemetryCommunication : HubCommunicationBase
    {
        /// <summary>
        /// The device connection string to be used to connect to IoTHub.
        /// </summary>
        public static string SaslUser { get; set; } = null;

        /// <summary>
        /// The IoTHub owner connection string to be used to connect to IoTHub.
        /// </summary>
        public static string SaslPassword { get; set; } = null;

        /// <summary>
        /// The IoTHub owner connection string to be used to connect to IoTHub.
        /// </summary>
        public static Uri AmqpTarget { get; set; } = null;

        /// <summary>
        /// Get the singleton.
        /// </summary>
        public static AmqpTelemetryCommunication Instance
        {
            get
            {
                lock (_singletonLock)
                {
                    if (_instance == null)
                    {
                        _instance = new AmqpTelemetryCommunication();
                    }
                    return _instance;
                }
            }
        }

        /// <summary>
        /// Ctor for the singleton class.
        /// </summary>
        private AmqpTelemetryCommunication()
        {
            // check if we got an IoTHub owner connection string
            if (string.IsNullOrEmpty(SaslUser) || string.IsNullOrEmpty(SaslPassword))
            {
                Logger.Information("No AMQP SASL PLAIN credentials given.");
            }

            // connect as device client
            this.hubClient = HubClient.CreateAmqpSender(AmqpTarget, SaslUser, SaslPassword);
            if (!InitHubCommunicationAsync(this.hubClient).Result)
            {
                string errorMessage = $"Cannot create IoTHub client. Exiting...";
                Logger.Fatal(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        private static readonly object _singletonLock = new object();
        private static AmqpTelemetryCommunication _instance = null;
        private IHubClient hubClient;
    }
}
