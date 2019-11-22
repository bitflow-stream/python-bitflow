import torch
from torch import nn
from torch.autograd import Variable
import torch.nn.functional as F


# appr. 8h training time
class GRUS2S(nn.Module):

    def __init__(self, input_dimensions, num_classes, hidden_dimensions=256, num_layers=1,
                 dropout=0.0, bidirectional=False):
        super(GRUS2S, self).__init__()
        self.input_dimensions = input_dimensions
        self.hidden_dimensions = hidden_dimensions

        self.num_classes = num_classes

        self.num_layers = num_layers
        self.dropout = dropout

        self.bidirectional = bidirectional

        self.h_previous = None  # Used to memorize hidden state when doing online sequence predictions

        # GRU Layer
        self.gru = nn.GRU(input_size=self.input_dimensions, hidden_size=self.hidden_dimensions,
                          num_layers=self.num_layers, dropout=self.dropout, batch_first=True,
                          bidirectional=self.bidirectional)

        # Fully connected for full seq output i.e. linear layer of fcs.
        # This will give me an output of sequence size more precisely: batch_size*seq_len*num_classes
        self.fc = nn.Linear(in_features=self.hidden_dimensions, out_features=self.num_classes)

    def forward(self, X):
        raise NotImplementedError("Use explicit forward_train or forward_eval methods.")

    def forward_train(self, X):
        # should be (M, S, N) --> M: Batch Size(if batch first), S: Seq Lenght, N: Number of features

        batch_index = 0
        num_direction = 2 if self.bidirectional else 1

        # Hidden state in first seq of the GRU
        h_0 = self.init_hidden(X.size(batch_index), num_direction)

        output_gru, h_n = self.gru(X, h_0)

        # nn.Linear operates on the last dimension of its input
        # i.e. for each slice [i, j, :] of gru_output it produces a vector of size num_classes
        fc_output = self.fc(output_gru)
        # Note that it is batch-first
        # Now each individual in the sequence will have prediction label.
        # fc_output = [self.fcs[i](output_gru[:, i, :]) for i in range(self.seq_len)]

        return fc_output  # output will be batch_size*num_classes for each entry

    def forward_eval(self, X_t, reset=False):
        # should be (1, S, N) --> S: Number of time steps in sequence, N: Number of features
        # Note that S can be 1, a subset or the whole sequence. It is important to set reset to True if a new sequence
        # prediction starts.

        num_direction = 2 if self.bidirectional else 1

        # Hidden state in first seq of the GRU
        if reset or self.h_previous is None:
            h_0 = self.init_hidden(1, num_direction)
        else:
            h_0 = self.h_previous

        output_gru, h_n = self.gru(X_t, h_0)

        self.h_previous = h_n

        # nn.Linear operates on the last dimension of its input
        # i.e. for each slice [i, j, :] of gru_output it produces a vector of size num_classes
        fc_output = self.fc(output_gru)
        # Note that it is batch-first
        # Now each individual in the sequence will have prediction label.
        # fc_output = [self.fcs[i](output_gru[:, i, :]) for i in range(self.seq_len)]
        for i in range(fc_output.size(0)):
            fc_output[i] = F.softmax(fc_output[i], dim=1)

        return fc_output  # output will be batch_size*num_classes for each entry

    def _init_hidden(self, batch_size, num_direction):
        h0 = Variable(torch.zeros((num_direction * self.num_layers, batch_size, self.hidden_dimensions),
                                  dtype=torch.float64))
        return h0
