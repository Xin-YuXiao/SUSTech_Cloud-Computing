import time

import numpy as np
from matplotlib import pyplot as plt

from calcTrilinearInterpWeights import calcTrilinearInterpWeights
from formCell2EdgeMatrix import formCell2EdgeMatrix
from formEdge2EdgeMatrix import formEdge2EdgeMatrix
from formFace2EdgeMatrix import formFace2EdgeMatrix
from formRectMeshConnectivity import formRectMeshConnectivity
from makeRectMeshModelBlocks import makeRectMeshModelBlocks
from solveRESnet import solveRESnet

if __name__ == '__main__':
    """
    Testing the solution of half-space
    In the following, a pole-dipole array is simulated for a uniform
    half-space and compared against the analytic solution.
    """

    '''Setup the 3D mesh'''
    # Create a 3D rectilinear mesh
    h = 2
    ratio = 1.14
    nctbc = 30
    tmp = np.cumsum(h * np.power(ratio, np.arange(nctbc + 1)))
    nodeX = np.round(np.concatenate((-tmp[::-1], [0], tmp)))  # node locations in X
    nodeY = np.round(np.concatenate((-tmp[::-1], [0], tmp)))  # node locations in Y
    nodeZ = np.round(np.concatenate(([0], -tmp)))  # node locations in Z

    '''Setup the geo-electrical model'''
    # Define the model using combination of blocks
    # A model includes some blocks that can represent objects like sheets or lines when one or two dimensions vanish.
    blkLoc = np.array([-np.inf, np.inf, -np.inf, np.inf, 0, -np.inf])  # a uniform half-space
    blkCon = np.array([1e-2])  # conductive property of the volumetric object (S/m)
    # Get conductive property model vectors (convert the block-model description to values on edges, faces and cells)
    cellCon, faceCon, edgeCon = makeRectMeshModelBlocks(nodeX, nodeY, nodeZ, blkLoc, blkCon, [], [], [])

    '''Setup the electric surveys (pole-dipole)'''
    # Define the current sources in the format of [x y z current(Ampere)]
    tx = np.array([[(0, 0, 0, 1),  # A electrode
                    [-np.inf, 0, 0, -1]]])  # B electrode

    # Define the receiver electrodes in the format of [Mx My Mz Nx Ny Nz]
    rx = np.array([[[10, 0, 0, 20, 0, 0],  # nine M-N pairs for the source
                    [20, 0, 0, 30, 0, 0],
                    [30, 0, 0, 40, 0, 0],
                    [40, 0, 0, 50, 0, 0],
                    [50, 0, 0, 60, 0, 0],
                    [60, 0, 0, 70, 0, 0],
                    [70, 0, 0, 80, 0, 0],
                    [80, 0, 0, 90, 0, 0],
                    [90, 0, 0, 100, 0, 0]]])


# Log in AWS
# Send variables (nodeX, nodeY, nodeZ, cellCon、faceCon、edgeCon、tx、rx) to S3
# Wait until Lambda finishes
# Get "data" back and run the following codes



    '''Compare against analytic solutions'''
    Aloc = np.array([0, 0, 0])  # location of A electrode
    rAM = rx[0][:, 0] - Aloc[0]  # A-M distance
    rAN = rx[0][:, 3] - Aloc[0]  # A-N distance
    rho = 100  # half-space resistivity
    I = 1
    dV = rho * I / 2 / np.pi * (1 / rAM - 1 / rAN)  # potential differences (analytic solution)
    X = 0.5 * (rx[0][:, 0] + rx[0][:, 3])  # centers of M-N (x-coordinate)

    fig, axs = plt.subplots(2, 1, figsize=(10, 10))
    axs[0].semilogy(X, data[0], '.-', label='RESnet')
    axs[0].plot(X, dV, 'o-', label='Analytic', markerfacecolor='none')
    axs[0].set_title('(a) Numerical and analytic solutions')
    axs[0].set_xlabel('Tx-Rx offset (m)')
    axs[0].set_ylabel('Potential difference (V)')
    axs[0].set_xlim(10, 100)
    axs[0].set_ylim(0.01, 1)
    axs[0].legend()
    axs[0].grid(True)

    axs[1].plot(X, ((data[0] - dV) / dV), 'k.-')
    axs[1].set_title('(b) Numerical errors')
    axs[1].set_xlabel('Tx-Rx offset (m)')
    axs[1].set_ylabel('Relative error')
    axs[1].set_xlim(10, 100)
    axs[1].set_ylim(-0.03, 0.02)
    axs[1].grid(True)

    plt.show()





