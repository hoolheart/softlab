"""
Signal process module

Public attributes:

- Signal, class, Abstract class representing a signal
- Wavement, class, A pair of a time sequence and value sequece
- FactorSignal, class, Any signal multiplied by a factor
- OffsetSignal, class, Any signal plus an offset
- SumSignal, class, Sum of several signals
- ConcatSignal, class, Concatenation of several signals
- ReciprocalSignal, class, Fraction of a factor above a signal
- FunctionalSignal, class, A signal defined by a given function
- FixedSignal, class, A signal with fixed value
- PeriodicSignal, class, Abstract class of any periodical signal
- SineSignal, class, A sine signal with real number
- ComplexSineSignal, class, A sine signal with complex number
- TriangleSignal, class, Periodic triangle signal
- RampSignal, class, Periodic ramp signal
- SquareSignal, class, Periodic square signal
- PulseSignal, class, Abstract class of any pulse-like signal
- ChirpSignal, class, Linear chirp signal
- ExpoChirpSignal, class, Exponential chirp signal
- Window, class, Abstract class of any window
- RectangleWindow, class, Rectangle window
- CosineWindow, class, Cosine window
- GaussianWindow, class, Gaussian window
- GaussianFlatTopWindow, class, Window with gaussian edge and flat top
- HanningWindow, class, Hanning window
- HammingWindow, class, Hamming window
- TriangleWindow, class, Triangle window
- ChebyshevWindow, class, Chebyshev window
- BlackmanWindow, class, Blackman window
- SlepianWindow, class, Slepian window (general gaussian window)
- Noise, class, Abstract class of any noise
- UniformNoise, class, Uncorrected uniform noise
- BrownianNoise, class, Brownian noise
- GaussianNoise, class, Uncorrected gaussian noise
- unbias, function, Remove bias of any sequence
- normalize, function, Normalize sequence into [-max, max] interval
- pad, function, Pad a sequence with given value
- sample_signal, function, Generate a wavement by sampling a signal
"""

from softlab.jin.sp.base import (
    Signal,
    Wavement,
    FactorSignal,
    OffsetSignal,
    SumSignal,
    ConcatSignal,
    ReciprocalSignal,
)

from softlab.jin.sp.common import (
    FunctionalSignal,
    FixedSignal,
    LinearSignal,
    PeriodicSignal,
    SineSignal,
    ComplexSineSignal,
    TriangleSignal,
    RampSignal,
    SquareSignal,
    PulseSignal,
    ChirpSignal,
    ExpoChirpSignal,
)

from softlab.jin.sp.window import (
    Window,
    RectangleWindow,
    CosineWindow,
    GaussianWindow,
    GaussianFlatTopWindow,
    HanningWindow,
    HammingWindow,
    TriangleWindow,
    ChebyshevWindow,
    BlackmanWindow,
    SlepianWindow,
)

from softlab.jin.sp.noise import (
    Noise,
    UniformNoise,
    BrownianNoise,
    GaussianNoise,
)

from softlab.jin.sp.operate import (
    unbias,
    normalize,
    pad,
    sample_signal,
)

from softlab.jin.sp.iq_modulation import (
    generate_iq_wavements,
    iq_modulation,
    iq_demodulation,
)
