"""
Model of van der Waals equation on gases and liquid

Major references:
- Prodanov, E.M., 2022. Mathematical analysis of the van der Waals equation.
  Physica B: Condensed Matter 640, 414077.
  https://doi.org/10.1016/j.physb.2022.414077
"""

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)
import pandas as pd
from scipy.constants import gas_constant
from softlab.tu.physics.physics_model import PhysicsModel
from softlab.jin.validator import ValNumber

class VanDerWaals(PhysicsModel):

    def __init__(self,
                 name: Optional[str] = None,
                 a: float = 0.0, b: float = 0.0) -> None:
        super().__init__(name)
        self.add_attribute('a', ValNumber(0.0), float(a))
        self.add_attribute('b', ValNumber(0.0), float(b))

    def get_calculator(self,
                       type: str,
                       conditions: Dict[str, Any] = ...) -> Callable[..., Any]:
        if type == 'pressure':
            return lambda x: self._pressure(x)
        elif type == 'pressure_isothermal':
            if not isinstance(conditions, Dict):
                raise TypeError(f'Invalid conditions {type(conditions)}')
            elif not 'temperature' in conditions:
                raise ValueError('No temperature in conditions')
            t = float(conditions['temperature'])
            if t > self.a() / gas_constant / self.b() / 4.0:
                return lambda x: self._isothermal(x, t)
            raise ValueError(f'Invalid temperature {t}')
        return super().get_calculator(type, conditions)

    def calculate_features(self) -> Dict[str, Any]:
        a = self.a()
        b = self.b()
        boyle = a / gas_constant / b
        return {
            'min_temperature': boyle / 4.0 ,
            'boyle_temperature': boyle,
            'critical_temperature': boyle * 8.0 / 27.0,
            'min_volumn': b,
            'critical_volumn': 3.0 * b,
            'critical_pressure': a / 27.0 / b / b,
        }

    def _pressure(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df, pd.DataFrame) and 'volumn' in df and \
            'temperature' in df:
          v = df.volumn
          t = df.temperature
          rst = pd.DataFrame()
          rst['pressure'] = gas_constant * t / (v - self.b()) - self.a() / v / v
          return rst
        raise ValueError(f'Invalid input {type(df)}')

    def _isothermal(self, df: pd.DataFrame, t: float) -> pd.DataFrame:
        if isinstance(df, pd.DataFrame) and 'volumn' in df:
          v = df.volumn
          rst = pd.DataFrame()
          rst['pressure'] = gas_constant * t / (v - self.b()) - self.a() / v / v
          return rst
        raise ValueError(f'Invalid input {type(df)}')

if __name__ == '__main__':
    m = VanDerWaals('gas01', 0.1, 0.01)
    print(f'Create van der Waals model {m}')
    print(f'Features: {m.features}')
    isothermal = m.get_calculator('pressure_isothermal', {'temperature': 300.0})
    df = pd.DataFrame({'volumn': [0.02, 0.03, 0.04, 0.05, 0.06]})
    print(f'Isothermal process: {isothermal(df)}')
    press = m.get_calculator('pressure')
    df['temperature'] = pd.Series([270.0, 280.0, 290.0, 300.0, 310.0])
    print(f'Pressure due to V, T: {press(df)}')
