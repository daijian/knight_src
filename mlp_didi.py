''' 
Didi Algorithm Program V1.0
Multilayer Perceptron implementation example using TensorFlow library.
'''

# Import didi Train data
import tensorflow as tf

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

filename = "complete_data.csv"

# setup text reader
file_length = file_len(filename)
filename_queue = tf.train.string_input_producer([filename])
reader = tf.TextLineReader()
_, csv_row = reader.read(filename_queue)

# setup CSV decoding
#schema structure:
#start_district_id, start_tj_level_str, start_poi_class_str, 
#dest_district_id, dest_tj_level_str, dest_poi_class_str,
#weather, temperature, pm25, time_spice, flag
record_defaults = [[""],[""],[""],[""],[""],[""],[0],[0.0],[0.0],[0],[0]]
start_district_id, start_tj_level, start_poi_class,\
dest_district_id, dest_tj_level, dest_poi_class, \
weather, temperature, pm25, time_spice, flag\
    = tf.decode_csv(csv_row, record_defaults=record_defaults)

# turn features back into a tensor
features = tf.pack([ 
        start_district_id, 
        start_tj_level, 
        start_poi_class, 
        dest_district_id, 
        dest_tj_level, 
        dest_poi_class, 
        weather, 
        temperature, 
        pm25,
        time_spice])

# Parameters
learning_rate = 0.001
training_epochs = 15
batch_size = 1000
display_step = 1

# Network Parameters
n_hidden_1 = 256 # 1st layer num features
n_hidden_2 = 256 # 2nd layer num features
n_input    = 10  # didi data input 
n_classes  = 2   # result total classes (0:no request 1:have request)

# tf Graph input
x = tf.placeholder("float", [None, n_input])
y = tf.placeholder("float", [None, n_classes])

# Create model
def multilayer_perceptron(_X, _weights, _biases):
    layer_1 = tf.nn.relu(tf.add(tf.matmul(_X, _weights['h1']), _biases['b1'])) #Hidden layer with RELU activation
    layer_2 = tf.nn.relu(tf.add(tf.matmul(layer_1, _weights['h2']), _biases['b2'])) #Hidden layer with RELU activation
    return tf.matmul(layer_2, _weights['out']) + _biases['out']

# Store layers weight & bias
weights = {
    'h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
    'h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'out': tf.Variable(tf.random_normal([n_hidden_2, n_classes]))
}
biases = {
    'b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'out': tf.Variable(tf.random_normal([n_classes]))
}

# Construct model
pred = multilayer_perceptron(x, weights, biases)

# Define loss and optimizer
cost = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(pred, y)) # Softmax loss
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate).minimize(cost) # Adam Optimizer

# Initializing the variables
init = tf.initialize_all_variables()

# Launch the graph
with tf.Session() as sess:
    sess.run(init)

    # Training cycle
    for epoch in range(training_epochs):
        avg_cost = 0.
        total_batch = int(1000000000/batch_size)
        # Loop over all batches
        for i in range(total_batch):
            batch_xs = features
            batch_ys = flag
            # Fit training using batch data
            sess.run(optimizer, feed_dict={x: batch_xs, y: batch_ys})
            # Compute average loss
            avg_cost += sess.run(cost, feed_dict={x: batch_xs, y: batch_ys})/total_batch
        # Display logs per epoch step
        if epoch % display_step == 0:
            print "Epoch:", '%04d' % (epoch+1), "cost=", "{:.9f}".format(avg_cost)

    print "Optimization Finished!"

    # Test model
    correct_prediction = tf.equal(tf.argmax(pred, 1), tf.argmax(y, 1))
    # Calculate accuracy
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))
    print "Accuracy:", accuracy.eval({x: mnist.test.images, y: mnist.test.labels})

