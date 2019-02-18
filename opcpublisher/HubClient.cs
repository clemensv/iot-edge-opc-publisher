namespace OpcPublisher
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.Amqp.Encoding;
    using Microsoft.Azure.Amqp.Framing;
    using Microsoft.Azure.Amqp.Sasl;
    using Microsoft.Azure.Devices.Client;

    /// <summary>
    ///     Class to encapsulate the IoTHub device/module client interface.
    /// </summary>
    public class HubClient : IHubClient, IDisposable
    {
        private readonly AmqpConnectionFactory _amqpClient;
        private readonly Uri amqpTarget;
        private readonly string saslPassword;
        private readonly string saslUser;
        private AmqpConnection _amqpConnection;
        private SendingAmqpLink _amqpLink;
        private AmqpSession _amqpSession;
        private readonly ModuleClient _edgeHubClient;

        private readonly DeviceClient _iotHubClient;

        /// <summary>
        ///     Ctor for the class.
        /// </summary>
        public HubClient()
        {
        }

        /// <summary>
        ///     Ctor for the class.
        /// </summary>
        private HubClient(DeviceClient iotHubClient)
        {
            _iotHubClient = iotHubClient;
        }

        /// <summary>
        ///     Ctor for the class.
        /// </summary>
        private HubClient(ModuleClient edgeHubClient)
        {
            _edgeHubClient = edgeHubClient;
        }

        private HubClient(AmqpConnectionFactory amqpConnectionFactory, Uri amqpTarget, string saslUser,
            string saslPassword)
        {
            _amqpClient = amqpConnectionFactory;
            this.amqpTarget = amqpTarget;
            this.saslUser = saslUser;
            this.saslPassword = saslPassword;
        }

        /// <summary>
        ///     Stores custom product information that will be appended to the user agent string that is sent to IoT Hub.
        /// </summary>
        public string ProductInfo
        {
            get
            {
                if (_amqpClient != null) return "Amqp";

                if (_iotHubClient == null) return _edgeHubClient.ProductInfo;
                return _iotHubClient.ProductInfo;
            }
            set
            {
                if (_amqpClient != null) return;
                if (_iotHubClient == null) _edgeHubClient.ProductInfo = value;
                _iotHubClient.ProductInfo = value;
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            // do cleanup
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Close the client instance
        /// </summary>
        public Task CloseAsync()
        {
            if (_amqpConnection != null) return _amqpConnection.CloseAsync(TimeSpan.FromSeconds(30));
            if (_iotHubClient == null) return _edgeHubClient.CloseAsync();
            return _iotHubClient.CloseAsync();
        }

        /// <summary>
        ///     Sets the retry policy used in the operation retries.
        /// </summary>
        public void SetRetryPolicy(IRetryPolicy retryPolicy)
        {
            if (_edgeHubClient != null)
            {
                _edgeHubClient.SetRetryPolicy(retryPolicy);
                return;
            }

            if (_iotHubClient != null) _iotHubClient.SetRetryPolicy(retryPolicy);
        }

        /// <summary>
        ///     Registers a new delegate for the connection status changed callback. If a delegate is already associated,
        ///     it will be replaced with the new delegate.
        /// </summary>
        public void SetConnectionStatusChangesHandler(ConnectionStatusChangesHandler statusChangesHandler)
        {
            if (_edgeHubClient != null)
            {
                _edgeHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
                return;
            }

            if (_iotHubClient != null) _iotHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
        }

        /// <summary>
        ///     Explicitly open the DeviceClient instance.
        /// </summary>
        public async Task OpenAsync()
        {
            if (_amqpClient != null)
            {
                _amqpConnection = await _amqpClient.OpenConnectionAsync(new UriBuilder(amqpTarget) { Path = "" }.Uri,
                    new SaslPlainHandler { AuthenticationIdentity = saslUser, Password = saslPassword },
                    TimeSpan.FromSeconds(30));
                _amqpSession = _amqpConnection.CreateSession(new AmqpSessionSettings());
                await _amqpSession.OpenAsync(TimeSpan.FromSeconds(30));
                _amqpLink = new SendingAmqpLink(_amqpSession, GetLinkSettings(true, amqpTarget.PathAndQuery.Substring(1), SettleMode.SettleOnSend));
                await _amqpLink.OpenAsync(TimeSpan.FromSeconds(30));
                return;
            }

            if (_edgeHubClient != null)
                await _edgeHubClient.OpenAsync();
            else if (_iotHubClient != null) await _iotHubClient.OpenAsync();
        }

        /// <summary>
        ///     Registers a new delegate for the named method. If a delegate is already associated with
        ///     the named method, it will be replaced with the new delegate.
        /// </summary>
        public Task SetMethodHandlerAsync(string methodName, MethodCallback methodHandler)
        {
            if (_amqpClient != null) return Task.CompletedTask;
            if (_iotHubClient == null)
                return _edgeHubClient.SetMethodHandlerAsync(methodName, methodHandler, _edgeHubClient);
            return _iotHubClient.SetMethodHandlerAsync(methodName, methodHandler, _iotHubClient);
        }

        /// <summary>
        ///     Registers a new delegate that is called for a method that doesn't have a delegate registered for its name.
        ///     If a default delegate is already registered it will replace with the new delegate.
        /// </summary>
        public Task SetMethodDefaultHandlerAsync(MethodCallback methodHandler)
        {
            if (_amqpClient != null) return Task.CompletedTask;

            if (_iotHubClient == null)
                return _edgeHubClient.SetMethodDefaultHandlerAsync(methodHandler, _edgeHubClient);
            return _iotHubClient.SetMethodDefaultHandlerAsync(methodHandler, _iotHubClient);
        }

        /// <summary>
        ///     Sends an event to device hub
        /// </summary>
        public async Task SendEventAsync(Message message)
        {
            try
            {
                if (_amqpLink != null)
                {
                    ((MemoryStream) message.BodyStream).Position = 0;


                    BinaryReader br = new BinaryReader(message.BodyStream);
                    var buf = br.ReadBytes((int) message.BodyStream.Length);
                    var amqpMessage = AmqpMessage.Create(new AmqpValue() {Value = buf});
                    foreach (var messageProperty in message.Properties)
                    {
                        amqpMessage.ApplicationProperties.Map.Add(messageProperty.Key, messageProperty.Value);
                    }


                    amqpMessage.Properties.AbsoluteExpiryTime = DateTime.UtcNow.AddHours(2); //(DateTime?)message.ExpiryTimeUtc;
                    //amqpMessage.Properties.ContentEncoding = new AmqpSymbol(message.ContentEncoding);
                    //amqpMessage.Properties.ContentType = new AmqpSymbol(message.ContentType);
                    //amqpMessage.Properties.CorrelationId = MessageId.(message.CorrelationId);
                    // amqpMessage.Properties.CreationTime = message.CreationTimeUtc;
                    //amqpMessage.Properties.MessageId = new AmqpSymbol(message.MessageId);
                    //amqpMessage.Properties.To = message.To;


                    var outcome = await _amqpLink.SendMessageAsync(amqpMessage, AmqpConstants.EmptyBinary,
                        AmqpConstants.EmptyBinary,
                        TimeSpan.MaxValue);
                    if (outcome.DescriptorCode != Accepted.Code)
                    {
                        throw new InvalidOperationException((string) outcome.Value);
                    }

                    return;
                }

                if (_iotHubClient == null) await _edgeHubClient.SendEventAsync(message);
                await _iotHubClient.SendEventAsync(message);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_amqpConnection != null)
                {
                    _amqpConnection.Close();
                    _amqpConnection = null;
                    return;
                }

                if (_iotHubClient == null)
                {
                    _edgeHubClient.Dispose();
                    return;
                }

                _iotHubClient.Dispose();
            }
        }


        /// <summary>
        ///     Create DeviceClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateDeviceClientFromConnectionString(string connectionString,
            TransportType transportType)
        {
            return new HubClient(DeviceClient.CreateFromConnectionString(connectionString, transportType));
        }

        /// <summary>
        ///     Create ModuleClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateModuleClientFromEnvironment(TransportType transportType)
        {
            return new HubClient(ModuleClient.CreateFromEnvironmentAsync(transportType).Result);
        }


        public static IHubClient CreateAmqpSender(Uri amqpTarget, string saslUser, string saslPassword)
        {
            var amqpConnectionFactory = new AmqpConnectionFactory();
            return new HubClient(amqpConnectionFactory, amqpTarget, saslUser, saslPassword);
        }

        public static AmqpLinkSettings GetLinkSettings(bool forSender, string address, SettleMode settleType,
            int credit = 0, bool dynamic = false)

        {
            AmqpLinkSettings settings = null;
            // create a setting for sender

            settings = new AmqpLinkSettings();
            settings.LinkName = string.Format("link-{0}", Guid.NewGuid().ToString("N"));
            settings.Role = !forSender;

            Target target = new Target();
            target.Address = address;

            Source source = new Source();
            source.Address = address;

            source.DistributionMode = "move";
            settings.Source = source;
            settings.Target = target;

            settings.SettleType = settleType;

            if (!forSender)
            {
                settings.TotalLinkCredit = (uint)credit;
                settings.AutoSendFlow = credit > 0;
                if (dynamic)
                {
                    source.Address = null;
                    source.Dynamic = true;
                }
            }
            else
            {
                settings.InitialDeliveryCount = 0;
                if (dynamic)
                {
                    target.Address = null;
                    target.Dynamic = true;
                }
            }
            settings.AddProperty("x-opt-just-testing", "ignore me");
            return settings;
        }
    }
}